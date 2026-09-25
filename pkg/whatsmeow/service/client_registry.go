package whatsmeow_service

import (
	"sync"
	"sync/atomic"

	"go.mau.fi/whatsmeow"
)

// Story 72.7 — concurrency patch (fork-local; keep when merging upstream).
//
// ClientRegistry is the single, mutex-guarded owner of the per-instance runtime
// state that used to live in three unsynchronized, process-wide maps
// (killChannel, clientPointer and myClientPointer). Concurrent reads and writes
// on those plain maps crashed the whole process twice in production with
// `fatal error: concurrent map read and map write`, taking every WhatsApp
// number down at once.
//
// Lock ordering: ClientRegistry.mu is a leaf lock. It is never held while
// calling into whatsmeow, the database, loggers, webhooks or any other lock
// (including reconnectCoordinator.mu from Story 72.5, which is also a leaf and
// is never held while touching the registry), so the two cannot deadlock.
type ClientRegistry struct {
	mu        sync.RWMutex
	clients   map[string]*whatsmeow.Client
	sessions  map[string]*InstanceSession
	myClients map[string]*MyClient

	// activeLoops counts StartClient supervisor loops currently parked waiting
	// for their session signal (observability for the post-deploy check).
	activeLoops atomic.Int64
}

// NewClientRegistry creates an empty registry shared by every service that
// needs the live whatsmeow client of an instance.
func NewClientRegistry() *ClientRegistry {
	return &ClientRegistry{
		clients:   make(map[string]*whatsmeow.Client),
		sessions:  make(map[string]*InstanceSession),
		myClients: make(map[string]*MyClient),
	}
}

// GetClient returns the live whatsmeow client of an instance, or nil.
func (r *ClientRegistry) GetClient(instanceID string) *whatsmeow.Client {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.clients[instanceID]
}

// LookupClient returns the live whatsmeow client of an instance and whether an
// entry exists (comma-ok form of GetClient).
func (r *ClientRegistry) LookupClient(instanceID string) (*whatsmeow.Client, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	client, ok := r.clients[instanceID]
	return client, ok
}

// DeleteClient unconditionally removes the client entry of an instance.
func (r *ClientRegistry) DeleteClient(instanceID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.clients, instanceID)
}

// DeleteClientIf removes the client entry only if it still points to client,
// so a late cleanup can never remove a newer client of the same instance.
func (r *ClientRegistry) DeleteClientIf(instanceID string, client *whatsmeow.Client) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if current, ok := r.clients[instanceID]; ok && current == client {
		delete(r.clients, instanceID)
		return true
	}
	return false
}

// SignalRestart asks the running StartClient loop of an instance to disconnect,
// clean up and start a fresh client (the historical kill-channel behaviour).
// The session stays registered: the loop replaces it when it restarts.
// Returns false if no session is registered or it was already signaled.
func (r *ClientRegistry) SignalRestart(instanceID string) bool {
	r.mu.RLock()
	session := r.sessions[instanceID]
	r.mu.RUnlock()
	if session == nil {
		return false
	}
	return session.signal(true)
}

// StopSession unregisters the session of an instance and tells its StartClient
// loop (and every other consumer of the session) to stop without restarting.
// Returns false if there was no session or it had already been signaled.
func (r *ClientRegistry) StopSession(instanceID string) bool {
	r.mu.Lock()
	session := r.sessions[instanceID]
	delete(r.sessions, instanceID)
	r.mu.Unlock()
	if session == nil {
		return false
	}
	return session.signal(false)
}

// Stats returns a consistent snapshot used by the runtime stats log.
func (r *ClientRegistry) Stats() (clients, sessions, activeLoops int) {
	r.mu.RLock()
	clients, sessions = len(r.clients), len(r.sessions)
	r.mu.RUnlock()
	return clients, sessions, int(r.activeLoops.Load())
}

// setClientForSession publishes client only while session is still the current
// session of the instance, so a superseded start can never overwrite the
// client of the session that replaced it.
func (r *ClientRegistry) setClientForSession(instanceID string, session *InstanceSession, client *whatsmeow.Client) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sessions[instanceID] != session {
		return false
	}
	r.clients[instanceID] = client
	return true
}

func (r *ClientRegistry) lookupMyClient(instanceID string) (*MyClient, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	mycli, ok := r.myClients[instanceID]
	return mycli, ok
}

func (r *ClientRegistry) deleteMyClient(instanceID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.myClients, instanceID)
}

func (r *ClientRegistry) setMyClientForSession(instanceID string, session *InstanceSession, mycli *MyClient) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sessions[instanceID] != session {
		return false
	}
	r.myClients[instanceID] = mycli
	return true
}

func (r *ClientRegistry) deleteMyClientIf(instanceID string, mycli *MyClient) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if current, ok := r.myClients[instanceID]; ok && current == mycli {
		delete(r.myClients, instanceID)
		return true
	}
	return false
}

// claimSession registers a new session for the instance.
//
// With prev == nil (a fresh StartClient) the new session replaces whatever was
// registered and the replaced session is told to stop, so an older loop for the
// same instance terminates instead of being orphaned (the old goroutine leak).
//
// With prev != nil (a restart of an existing loop) the new session is only
// installed if prev is still the registered session; otherwise somebody else
// took over or removed the instance and nil is returned so the loop exits.
func (r *ClientRegistry) claimSession(instanceID string, prev *InstanceSession) *InstanceSession {
	next := newInstanceSession()
	r.mu.Lock()
	current := r.sessions[instanceID]
	if prev != nil && current != prev {
		r.mu.Unlock()
		return nil
	}
	r.sessions[instanceID] = next
	r.mu.Unlock()
	if current != nil && current != prev {
		current.signal(false)
	}
	return next
}

// releaseSession unregisters session if it is still the current one.
func (r *ClientRegistry) releaseSession(instanceID string, session *InstanceSession) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sessions[instanceID] == session {
		delete(r.sessions, instanceID)
	}
}

// superviseInstance is the StartClient loop. It replaces the old 1-second
// polling of the shared killChannel map and the recursive StartClient call on
// the kill path with an explicit loop:
//
//	claim session → start → block on session.Done() → onSignal → restart or exit
//
// The session reference is taken once per iteration and never re-read from the
// map, so the loop does no map access while waiting. alreadyRunning mirrors the
// historical "client exists and is connected" guard of StartClient. start
// returns the signal handler (which reports whether the loop should restart) or
// ok=false when the client could not be started.
func (r *ClientRegistry) superviseInstance(
	instanceID string,
	alreadyRunning func() bool,
	start func(session *InstanceSession) (onSignal func(restart bool) bool, ok bool),
) {
	var prev *InstanceSession
	for {
		if alreadyRunning() {
			if prev != nil {
				r.releaseSession(instanceID, prev)
			}
			return
		}

		session := r.claimSession(instanceID, prev)
		if session == nil {
			return
		}

		onSignal, ok := start(session)
		if !ok {
			r.releaseSession(instanceID, session)
			return
		}

		r.activeLoops.Add(1)
		<-session.Done()
		r.activeLoops.Add(-1)

		restart := session.restartRequested()
		if !onSignal(restart) || !restart {
			r.releaseSession(instanceID, session)
			return
		}
		prev = session
	}
}

// InstanceSession is the per-instance stop signal that replaces the old
// `chan bool` kill channel. It is a broadcast: signaling closes Done(), so every
// consumer (the StartClient loop and schedulePresenceUpdates) observes it and no
// consumer can "steal" a single send meant for another one. Signaling is
// idempotent and never blocks, so a signal can neither be lost nor hang the
// sender.
type InstanceSession struct {
	done    chan struct{}
	once    sync.Once
	restart bool // written once before done is closed; read only after <-done
}

func newInstanceSession() *InstanceSession {
	return &InstanceSession{done: make(chan struct{})}
}

// Done is closed when the session is signaled to restart or stop.
func (s *InstanceSession) Done() <-chan struct{} {
	return s.done
}

// signal fires the session once; the first signal decides restart vs stop.
func (s *InstanceSession) signal(restart bool) bool {
	if s == nil {
		return false
	}
	fired := false
	s.once.Do(func() {
		s.restart = restart
		close(s.done)
		fired = true
	})
	return fired
}

// restartRequested must only be called after Done() is closed.
func (s *InstanceSession) restartRequested() bool {
	<-s.done
	return s.restart
}
