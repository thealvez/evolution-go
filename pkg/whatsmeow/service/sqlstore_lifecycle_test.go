package whatsmeow_service

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/evolution-foundation/evolution-go/pkg/config"
	"go.mau.fi/whatsmeow/proto/waAdv"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	_ "modernc.org/sqlite"
)

func newSQLiteStoreService(t *testing.T, createDataDir bool) whatsmeowService {
	t.Helper()

	exPath := t.TempDir()
	if createDataDir {
		if err := os.Mkdir(filepath.Join(exPath, "dbdata"), 0o755); err != nil {
			t.Fatalf("create dbdata directory: %v", err)
		}
	}

	return whatsmeowService{
		config:   &config.Config{},
		exPath:   exPath,
		sqlStore: &sqlStoreState{},
	}
}

func testDeviceIdentity() *waAdv.ADVSignedDeviceIdentity {
	return &waAdv.ADVSignedDeviceIdentity{
		Details:             []byte{1},
		AccountSignatureKey: make([]byte, 32),
		AccountSignature:    make([]byte, 64),
		DeviceSignature:     make([]byte, 64),
	}
}

func TestGetSQLStoreContainerSharesOneContainerAcrossConcurrentStarts(t *testing.T) {
	service := newSQLiteStoreService(t, true)

	const callers = 100
	containers := make(chan *sqlstore.Container, callers)
	errorsCh := make(chan error, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			container, err := service.getSQLStoreContainer(nil)
			if err != nil {
				errorsCh <- err
				return
			}
			containers <- container
		}()
	}
	wg.Wait()
	close(containers)
	close(errorsCh)

	for err := range errorsCh {
		t.Fatalf("initialize shared container: %v", err)
	}

	var first *sqlstore.Container
	for container := range containers {
		if first == nil {
			first = container
			continue
		}
		if container != first {
			t.Fatal("expected every caller to receive the same SQL container")
		}
	}
	if first == nil {
		t.Fatal("expected a SQL container")
	}
	t.Cleanup(func() { _ = first.Close() })
}

func TestGetSQLStoreContainerRetriesAfterInitializationFailure(t *testing.T) {
	service := newSQLiteStoreService(t, false)

	if _, err := service.getSQLStoreContainer(nil); err == nil {
		t.Fatal("expected initialization to fail while dbdata directory is missing")
	}
	if service.sqlStore.container != nil {
		t.Fatal("failed initialization must not cache a container")
	}

	if err := os.Mkdir(filepath.Join(service.exPath, "dbdata"), 0o755); err != nil {
		t.Fatalf("create dbdata directory: %v", err)
	}
	container, err := service.getSQLStoreContainer(nil)
	if err != nil {
		t.Fatalf("retry shared container initialization: %v", err)
	}
	t.Cleanup(func() { _ = container.Close() })
}

func TestSharedSQLStoreKeepsDeviceSessionsIsolated(t *testing.T) {
	service := newSQLiteStoreService(t, true)
	container, err := service.getSQLStoreContainer(nil)
	if err != nil {
		t.Fatalf("initialize shared container: %v", err)
	}
	t.Cleanup(func() { _ = container.Close() })

	firstJID := types.NewJID("551100000001", types.DefaultUserServer)
	secondJID := types.NewJID("551100000002", types.DefaultUserServer)
	first := container.NewDevice()
	first.ID = &firstJID
	first.Account = testDeviceIdentity()
	if err := first.Save(context.Background()); err != nil {
		t.Fatalf("save first device: %v", err)
	}
	second := container.NewDevice()
	second.ID = &secondJID
	second.Account = testDeviceIdentity()
	if err := second.Save(context.Background()); err != nil {
		t.Fatalf("save second device: %v", err)
	}

	loadedFirst, err := container.GetDevice(context.Background(), firstJID)
	if err != nil {
		t.Fatalf("load first device: %v", err)
	}
	loadedSecond, err := container.GetDevice(context.Background(), secondJID)
	if err != nil {
		t.Fatalf("load second device: %v", err)
	}
	if loadedFirst == nil || loadedFirst.GetJID() != firstJID {
		t.Fatalf("first device session was not isolated: %#v", loadedFirst)
	}
	if loadedSecond == nil || loadedSecond.GetJID() != secondJID {
		t.Fatalf("second device session was not isolated: %#v", loadedSecond)
	}
	if loadedFirst.GetJID() == loadedSecond.GetJID() {
		t.Fatal("distinct instances resolved to the same device session")
	}
}

func TestReconnectCoordinatorSkipsDuplicateInstance(t *testing.T) {
	coordinator := &reconnectCoordinator{active: make(map[string]struct{})}
	entered := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)

	go func() {
		_, err := coordinator.run("instance-a", func() error {
			close(entered)
			<-release
			return nil
		})
		firstDone <- err
	}()
	<-entered

	var duplicateRuns atomic.Int32
	const duplicates = 50
	var wg sync.WaitGroup
	for range duplicates {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ran, err := coordinator.run("instance-a", func() error {
				duplicateRuns.Add(1)
				return nil
			})
			if err != nil {
				t.Errorf("duplicate reconnect: %v", err)
			}
			if ran {
				t.Error("duplicate reconnect unexpectedly ran")
			}
		}()
	}
	wg.Wait()
	if duplicateRuns.Load() != 0 {
		t.Fatalf("duplicate reconnect callback ran %d times", duplicateRuns.Load())
	}

	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first reconnect: %v", err)
	}
}

func TestReconnectCoordinatorAllowsOtherInstancesAndReleasesAfterError(t *testing.T) {
	coordinator := &reconnectCoordinator{active: make(map[string]struct{})}
	entered := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		_, _ = coordinator.run("instance-a", func() error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered

	otherRan := make(chan struct{})
	go func() {
		_, _ = coordinator.run("instance-b", func() error {
			close(otherRan)
			return nil
		})
	}()
	select {
	case <-otherRan:
	case <-time.After(time.Second):
		t.Fatal("different instance reconnect was unnecessarily serialized")
	}

	close(release)
	<-firstDone

	expected := errors.New("temporary reconnect failure")
	ran, err := coordinator.run("instance-c", func() error { return expected })
	if !ran || !errors.Is(err, expected) {
		t.Fatalf("expected first reconnect error, ran=%v err=%v", ran, err)
	}
	ran, err = coordinator.run("instance-c", func() error { return nil })
	if !ran || err != nil {
		t.Fatalf("guard was not released after error, ran=%v err=%v", ran, err)
	}
}
