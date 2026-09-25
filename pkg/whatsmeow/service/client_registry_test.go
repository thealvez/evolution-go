package whatsmeow_service

// Story 72.7 — concurrency tests for the ClientRegistry / StartClient lifecycle.
//
// These tests drive the real production code paths (StartInstance, StartClient
// → superviseInstance, reconnectClient + the Story 72.5 reconnect guard,
// teardownQR, the events.Disconnected handler, handleSessionSignal and
// schedulePresenceUpdates). Only the websocket start of each StartClient
// iteration is replaced (sessionStarter), because it needs a real WhatsApp
// connection and writes package-global whatsmeow DeviceProps.
//
// Leak detection technique: runtime.NumGoroutine() measured after a warm-up
// cycle and compared, after a stabilization wait, with the value at the end;
// plus an exact count of goroutines whose stack contains superviseInstance
// (the StartClient loop, whatsmeow.go:739 in the incident dumps) or
// schedulePresenceUpdates.

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickmn/go-cache"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types/events"

	"github.com/evolution-foundation/evolution-go/pkg/config"
	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	logger_wrapper "github.com/evolution-foundation/evolution-go/pkg/logger"
)

// ---------------------------------------------------------------------------
// fakes / harness

type fakeInstanceRepo struct {
	mu        sync.Mutex
	instances map[string]instance_model.Instance
	connected map[string]int
	qrcodes   map[string]int
}

func newFakeInstanceRepo(ids ...string) *fakeInstanceRepo {
	repo := &fakeInstanceRepo{
		instances: make(map[string]instance_model.Instance),
		connected: make(map[string]int),
		qrcodes:   make(map[string]int),
	}
	for _, id := range ids {
		repo.instances[id] = instance_model.Instance{Id: id, Name: "name-" + id, Token: "token-" + id}
	}
	return repo
}

func (f *fakeInstanceRepo) GetInstanceByID(id string) (*instance_model.Instance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	inst, ok := f.instances[id]
	if !ok {
		return nil, errors.New("not found")
	}
	return &inst, nil // fresh copy per call, like a DB read
}

func (f *fakeInstanceRepo) UpdateConnected(id string, _ bool, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.connected[id]++
	return nil
}

func (f *fakeInstanceRepo) UpdateQrcode(id string, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.qrcodes[id]++
	return nil
}

func (f *fakeInstanceRepo) Create(instance_model.Instance) (*instance_model.Instance, error) {
	return nil, errors.New("not implemented")
}
func (f *fakeInstanceRepo) GetConnectedInstanceByID(id string) (*instance_model.Instance, error) {
	return f.GetInstanceByID(id)
}
func (f *fakeInstanceRepo) GetInstanceByToken(string) (*instance_model.Instance, error) {
	return nil, errors.New("not implemented")
}
func (f *fakeInstanceRepo) GetInstanceByName(string) (*instance_model.Instance, error) {
	return nil, errors.New("not implemented")
}
func (f *fakeInstanceRepo) Update(*instance_model.Instance) error { return nil }
func (f *fakeInstanceRepo) UpdateProxy(string, string) error      { return nil }
func (f *fakeInstanceRepo) UpdateJid(string, string) error        { return nil }
func (f *fakeInstanceRepo) GetAllConnectedInstances() ([]*instance_model.Instance, error) {
	return nil, nil
}
func (f *fakeInstanceRepo) GetAllConnectedInstancesByClientName(string) ([]*instance_model.Instance, error) {
	return nil, nil
}
func (f *fakeInstanceRepo) GetAll(string) ([]*instance_model.Instance, error) { return nil, nil }
func (f *fakeInstanceRepo) Delete(string) error                               { return nil }
func (f *fakeInstanceRepo) GetAdvancedSettings(string) (*instance_model.AdvancedSettings, error) {
	return nil, errors.New("not implemented")
}
func (f *fakeInstanceRepo) UpdateAdvancedSettings(string, *instance_model.AdvancedSettings) error {
	return nil
}

type lifecycleHarness struct {
	t         *testing.T
	svc       *whatsmeowService
	repo      *fakeInstanceRepo
	container *sqlstore.Container

	starts atomic.Int64
	mu     sync.Mutex
	depths []int
	mycli  map[string]*MyClient
}

func newLifecycleHarness(t *testing.T, ids ...string) *lifecycleHarness {
	t.Helper()

	prevDelay := reconnectSettleDelay
	reconnectSettleDelay = time.Millisecond
	t.Cleanup(func() { reconnectSettleDelay = prevDelay })

	storeSvc := newSQLiteStoreService(t, true)
	container, err := storeSvc.getSQLStoreContainer(nil)
	if err != nil {
		t.Fatalf("initialize sqlite store: %v", err)
	}
	t.Cleanup(func() { _ = container.Close() })

	cfg := &config.Config{LogDirectory: t.TempDir(), QrcodeMaxCount: 5}
	repo := newFakeInstanceRepo(ids...)
	h := &lifecycleHarness{
		t:         t,
		repo:      repo,
		container: container,
		mycli:     make(map[string]*MyClient),
	}
	h.svc = &whatsmeowService{
		instanceRepository: repo,
		config:             cfg,
		userInfoCache:      cache.New(time.Minute, 0),
		clients:            NewClientRegistry(),
		callClients:        &sync.Map{},
		loggerWrapper:      logger_wrapper.NewLoggerManager(cfg),
		sqlStore:           &sqlStoreState{},
		reconnects:         &reconnectCoordinator{active: make(map[string]struct{})},
	}
	h.svc.sessionStarter = h.start
	return h
}

// start is the fake websocket start: an unpaired device (Store.ID == nil, like
// the instance of the incident) registered exactly like startClientSession does,
// plus the presence loop that the Connected handler starts for alwaysOnline.
func (h *lifecycleHarness) start(cd *ClientData, session *InstanceSession) (func(bool) bool, bool) {
	pcs := make([]uintptr, 512)
	depth := runtime.Callers(0, pcs)

	w := *h.svc
	client := whatsmeow.NewClient(h.container.NewDevice(), nil)
	w.clients.setClientForSession(cd.Instance.Id, session, client)
	mycli := w.newMyClient(cd, client, session)
	w.clients.setMyClientForSession(cd.Instance.Id, session, mycli)
	go schedulePresenceUpdates(mycli)

	h.mu.Lock()
	h.depths = append(h.depths, depth)
	h.mycli[cd.Instance.Id] = mycli
	h.mu.Unlock()
	h.starts.Add(1)

	return func(restart bool) bool {
		return w.handleSessionSignal(cd, client, mycli, nil, restart)
	}, true
}

func (h *lifecycleHarness) current(id string) *MyClient {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.mycli[id]
}

func (h *lifecycleHarness) reconnectInFlight() bool {
	h.svc.reconnects.mu.Lock()
	defer h.svc.reconnects.mu.Unlock()
	return len(h.svc.reconnects.active) > 0
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// countGoroutinesIn counts live goroutines whose stack contains fn.
func countGoroutinesIn(fn string) int {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	count := 0
	for _, g := range strings.Split(string(buf), "\n\n") {
		if strings.Contains(g, fn) {
			count++
		}
	}
	return count
}

const (
	// Stack traces print the import path, not the package name
	// (".../pkg/whatsmeow/service.", not "whatsmeow_service.").
	supervisorFrame = "/pkg/whatsmeow/service.(*ClientRegistry).superviseInstance("
	presenceFrame   = "/pkg/whatsmeow/service.schedulePresenceUpdates("
)

// settledGoroutines waits for transient goroutines (webhook dispatch, loggers)
// to finish and returns the stable runtime.NumGoroutine() value.
func settledGoroutines() int {
	last := runtime.NumGoroutine()
	stable := 0
	for i := 0; i < 400 && stable < 20; i++ {
		time.Sleep(5 * time.Millisecond)
		n := runtime.NumGoroutine()
		if n == last {
			stable++
		} else {
			stable = 0
			last = n
		}
	}
	return last
}

// ---------------------------------------------------------------------------
// ClientRegistry unit tests

func TestInstanceSessionSignalIsIdempotentBroadcastAndFirstSignalWins(t *testing.T) {
	session := newInstanceSession()
	if !session.signal(true) {
		t.Fatal("first signal must fire")
	}
	if session.signal(false) {
		t.Fatal("second signal must be a no-op")
	}
	for range 3 { // closed channel: every consumer observes it
		select {
		case <-session.Done():
		default:
			t.Fatal("Done must be closed after signal")
		}
	}
	if !session.restartRequested() {
		t.Fatal("first signal (restart) must win")
	}
	var nilSession *InstanceSession
	if nilSession.signal(true) {
		t.Fatal("signal on nil session must be a no-op")
	}
}

func TestClientRegistryCompareAndDeleteNeverRemovesNewerEntries(t *testing.T) {
	r := NewClientRegistry()
	old := r.claimSession("a", nil)
	oldClient := &whatsmeow.Client{}
	oldMy := &MyClient{}
	if !r.setClientForSession("a", old, oldClient) || !r.setMyClientForSession("a", old, oldMy) {
		t.Fatal("current session must be able to publish its client")
	}

	next := r.claimSession("a", nil)
	select {
	case <-old.Done():
	default:
		t.Fatal("replaced session must be stopped, not orphaned")
	}
	if old.restartRequested() {
		t.Fatal("replaced session must be stopped without restart")
	}
	if r.setClientForSession("a", old, &whatsmeow.Client{}) {
		t.Fatal("superseded session must not overwrite the client")
	}
	newClient := &whatsmeow.Client{}
	newMy := &MyClient{}
	r.setClientForSession("a", next, newClient)
	r.setMyClientForSession("a", next, newMy)

	if r.DeleteClientIf("a", oldClient) || r.deleteMyClientIf("a", oldMy) {
		t.Fatal("late cleanup of the old iteration removed the newer entries")
	}
	if r.GetClient("a") != newClient {
		t.Fatal("newer client lost")
	}
	if mycli, _ := r.lookupMyClient("a"); mycli != newMy {
		t.Fatal("newer MyClient lost")
	}

	// Restart claim only succeeds while prev is still current.
	if r.claimSession("a", old) != nil {
		t.Fatal("restart of a superseded session must not claim the instance")
	}
	if !r.StopSession("a") || r.StopSession("a") {
		t.Fatal("StopSession must fire exactly once")
	}
	if r.claimSession("a", next) != nil {
		t.Fatal("restart after StopSession must not resurrect the instance")
	}
	if r.SignalRestart("missing") || r.StopSession("missing") {
		t.Fatal("signals to unknown instances must be no-ops")
	}
}

func TestClientRegistryConcurrentAccessIsRaceFree(t *testing.T) {
	r := NewClientRegistry()
	var wg sync.WaitGroup
	for g := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := fmt.Sprintf("inst-%d", g%4)
			for i := range 500 {
				switch i % 8 {
				case 0:
					s := r.claimSession(id, nil)
					r.setClientForSession(id, s, &whatsmeow.Client{})
					r.setMyClientForSession(id, s, &MyClient{})
				case 1:
					_ = r.GetClient(id)
				case 2:
					_, _ = r.LookupClient(id)
				case 3:
					r.SignalRestart(id)
				case 4:
					r.StopSession(id)
				case 5:
					_, _ = r.lookupMyClient(id)
				case 6:
					r.DeleteClient(id)
					r.deleteMyClient(id)
				case 7:
					_, _, _ = r.Stats()
				}
			}
		}()
	}
	wg.Wait()
}

// ---------------------------------------------------------------------------
// AC3: both consumers of the stop signal receive it

func TestSessionSignalReachesStartClientLoopAndPresenceLoop(t *testing.T) {
	for _, tc := range []struct {
		name   string
		signal func(r *ClientRegistry, id string) bool
	}{
		{"stop (reconnect/delete)", func(r *ClientRegistry, id string) bool { return r.StopSession(id) }},
		{"restart (QR timeout/LoggedOut)", func(r *ClientRegistry, id string) bool { return r.SignalRestart(id) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := NewClientRegistry()
			presenceDone := make(chan struct{})
			started := make(chan *InstanceSession, 2)
			handled := make(chan bool, 2)
			superviseDone := make(chan struct{})
			starts := 0

			go func() {
				defer close(superviseDone)
				r.superviseInstance("a", func() bool { return false }, func(s *InstanceSession) (func(bool) bool, bool) {
					starts++
					if starts == 1 {
						go func() {
							schedulePresenceUpdates(&MyClient{userID: "a", session: s, loggerWrapper: logger_wrapper.NewLoggerManager(&config.Config{LogDirectory: t.TempDir()})})
							close(presenceDone)
						}()
					}
					started <- s
					return func(restart bool) bool {
						handled <- restart
						return starts < 2 // restart once at most
					}, true
				})
			}()

			first := <-started
			waitFor(t, "loop parked", func() bool { _, _, loops := r.Stats(); return loops == 1 })
			if !tc.signal(r, "a") {
				t.Fatal("signal did not fire")
			}

			select {
			case <-presenceDone:
			case <-time.After(5 * time.Second):
				t.Fatal("schedulePresenceUpdates did not receive the signal")
			}
			select {
			case restart := <-handled:
				if restart != first.restartRequested() {
					t.Fatal("loop observed a different signal kind")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("StartClient loop did not receive the signal")
			}
			if first.restartRequested() {
				second := <-started
				if second == first {
					t.Fatal("restart must use a fresh session")
				}
				r.StopSession("a")
			}
			select {
			case <-superviseDone:
			case <-time.After(5 * time.Second):
				t.Fatal("supervisor did not exit")
			}
			if _, sessions, loops := r.Stats(); sessions != 0 || loops != 0 {
				t.Fatalf("registry not clean: sessions=%d loops=%d", sessions, loops)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// AC4: no recursion — constant call depth across kill/restart cycles

func TestStartClientRestartsWithoutRecursion(t *testing.T) {
	h := newLifecycleHarness(t, "inst-a")
	inst, _ := h.repo.GetInstanceByID("inst-a")
	done := make(chan struct{})
	go func() {
		h.svc.StartClient(&ClientData{Instance: inst})
		close(done)
	}()
	waitFor(t, "first start", func() bool { return h.starts.Load() == 1 })

	const cycles = 60
	for i := 1; i <= cycles; i++ {
		waitFor(t, "loop parked", func() bool { _, _, loops := h.svc.clients.Stats(); return loops == 1 })
		h.current("inst-a").teardownQR("", false) // real QR-timeout path
		want := int64(i + 1)
		waitFor(t, "restart", func() bool { return h.starts.Load() == want })
	}

	h.mu.Lock()
	depths := append([]int(nil), h.depths...)
	h.mu.Unlock()
	for i, d := range depths {
		if d != depths[0] {
			t.Fatalf("call depth changed across restarts: start #%d depth=%d, first=%d", i, d, depths[0])
		}
	}
	if n := countGoroutinesIn(supervisorFrame); n != 1 {
		t.Fatalf("expected exactly 1 StartClient loop, got %d", n)
	}

	h.svc.clients.StopSession("inst-a")
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("StartClient did not return after StopSession")
	}
}

// ---------------------------------------------------------------------------
// AC3 + AC5: the incident cycle — unpaired instance → QR → QRTimeout → kill →
// Disconnected → reconnect, repeated, without goroutine growth.

func TestUnpairedQRTimeoutDisconnectReconnectCycleDoesNotLeak(t *testing.T) {
	const id = "dac3fbe4-unpaired"
	h := newLifecycleHarness(t, id)

	if err := h.svc.StartInstance(id); err != nil {
		t.Fatalf("StartInstance: %v", err)
	}
	waitFor(t, "first start", func() bool { return h.starts.Load() == 1 })

	expected := int64(1)
	cycle := func() {
		// QR generated, never scanned → QR codes exhausted → QRTimeout → kill.
		waitFor(t, "loop parked", func() bool { _, _, loops := h.svc.clients.Stats(); return loops == 1 })
		mycli := h.current(id)
		if mycli.WAClient.Store.ID != nil {
			t.Fatal("test instance must be unpaired")
		}
		mycli.teardownQR("", false)
		expected++
		waitFor(t, "restart after QR timeout", func() bool { return h.starts.Load() == expected })

		// Forced Disconnected (non-deterministic in production) → reconnect.
		waitFor(t, "loop parked", func() bool { _, _, loops := h.svc.clients.Stats(); return loops == 1 })
		h.current(id).myEventHandler(&events.Disconnected{})
		expected++
		waitFor(t, "restart after reconnect", func() bool { return h.starts.Load() == expected })
		waitFor(t, "reconnect finished", func() bool { return !h.reconnectInFlight() })
	}

	cycle() // warm-up: per-instance logger, sqlite, cache janitor, etc.
	waitFor(t, "loop parked", func() bool { _, _, loops := h.svc.clients.Stats(); return loops == 1 })
	baseline := settledGoroutines()

	const cycles = 60
	for range cycles {
		cycle()
	}

	waitFor(t, "loop parked", func() bool { _, _, loops := h.svc.clients.Stats(); return loops == 1 })
	after := settledGoroutines()
	if n := countGoroutinesIn(supervisorFrame); n != 1 {
		t.Fatalf("StartClient loops alive after %d cycles: %d (want 1 = active instances)", cycles, n)
	}
	if n := countGoroutinesIn(presenceFrame); n != 1 {
		t.Fatalf("presence loops alive after %d cycles: %d (want 1)", cycles, n)
	}
	// Allow a small margin for runtime/background goroutines; the old code grew
	// by at least one goroutine per reconnect (60+ here).
	if after > baseline+3 {
		t.Fatalf("goroutines grew from %d to %d over %d QR/reconnect cycles", baseline, after, cycles)
	}
	if clients, sessions, loops := h.svc.clients.Stats(); clients != 1 || sessions != 1 || loops != 1 {
		t.Fatalf("registry drifted: clients=%d sessions=%d loops=%d", clients, sessions, loops)
	}

	h.svc.clients.StopSession(id)
	waitFor(t, "loop exit", func() bool { return countGoroutinesIn(supervisorFrame) == 0 })
}

// ---------------------------------------------------------------------------
// AC1 + AC5: concurrent reconnects of several instances, with concurrent
// readers (the 9 service packages) — must be race-free under -race, with no
// deadlock against the Story 72.5 reconnect guard and no leaked loops.

func TestConcurrentReconnectsOfManyInstancesAreRaceFreeAndDoNotLeak(t *testing.T) {
	const instances = 8
	const reconnectsPerInstance = 25

	ids := make([]string, instances)
	for i := range ids {
		ids[i] = fmt.Sprintf("inst-%02d", i)
	}
	h := newLifecycleHarness(t, ids...)

	for _, id := range ids {
		if err := h.svc.StartInstance(id); err != nil {
			t.Fatalf("StartInstance(%s): %v", id, err)
		}
	}
	waitFor(t, "all loops parked", func() bool { _, _, loops := h.svc.clients.Stats(); return loops == instances })
	baseline := settledGoroutines()

	stopReaders := make(chan struct{})
	var readers sync.WaitGroup
	for range 4 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stopReaders:
					return
				default:
				}
				for _, id := range ids {
					_ = h.svc.clients.GetClient(id)
					_, _ = h.svc.clients.LookupClient(id)
					_, _ = h.svc.clients.lookupMyClient(id)
					_, _ = h.svc.GetCallClient(id)
				}
				_, _, _ = h.svc.clients.Stats()
			}
		}()
	}

	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range reconnectsPerInstance {
				if err := h.svc.ReconnectClient(id); err != nil {
					t.Errorf("ReconnectClient(%s): %v", id, err)
					return
				}
				if i%5 == 0 {
					if mycli := h.current(id); mycli != nil {
						mycli.teardownQR("", false) // restart racing with the next reconnect
					}
				}
			}
		}()
	}
	wg.Wait()
	close(stopReaders)
	readers.Wait()

	waitFor(t, "no reconnect in flight", func() bool { return !h.reconnectInFlight() })
	waitFor(t, "one loop per instance", func() bool {
		_, _, loops := h.svc.clients.Stats()
		return loops == instances && countGoroutinesIn(supervisorFrame) == instances
	})
	after := settledGoroutines()
	if after > baseline+3 {
		t.Fatalf("goroutines grew from %d to %d after %d reconnects", baseline, after, instances*reconnectsPerInstance)
	}
	if n := countGoroutinesIn(presenceFrame); n != instances {
		t.Fatalf("presence loops alive: %d (want %d)", n, instances)
	}
	if clients, sessions, _ := h.svc.clients.Stats(); clients != instances || sessions != instances {
		t.Fatalf("registry drifted: clients=%d sessions=%d (want %d)", clients, sessions, instances)
	}

	for _, id := range ids {
		h.svc.clients.StopSession(id)
	}
	waitFor(t, "all loops exit", func() bool { return countGoroutinesIn(supervisorFrame) == 0 })
}

// ---------------------------------------------------------------------------
// AC6: runtime stats instrumentation

func TestRuntimeStatsReportsLoopsAndStops(t *testing.T) {
	r := NewClientRegistry()
	s := r.claimSession("a", nil)
	r.setClientForSession("a", s, &whatsmeow.Client{})
	r.activeLoops.Add(1)
	defer r.activeLoops.Add(-1)

	format, args := formatRuntimeStats(r)
	line := fmt.Sprintf(format, args...)
	if !strings.Contains(line, "[runtime-stats] goroutines=") ||
		!strings.Contains(line, "startclient_loops=1 sessions=1 clients=1") {
		t.Fatalf("unexpected stats line: %q", line)
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		LogRuntimeStats(r, time.Millisecond, stop)
		close(done)
	}()
	time.Sleep(5 * time.Millisecond)
	close(stop)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("LogRuntimeStats did not stop")
	}
}
