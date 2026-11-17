package registry

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type recoveryWatch struct {
	events  chan Event
	errs    chan error
	stopped chan struct{}
}

type recoveryRegistry struct {
	mu             sync.Mutex
	member         Member
	listCalls      int
	resumeSnapshot chan struct{}
	watches        chan *recoveryWatch
}

func (r *recoveryRegistry) Register(_ context.Context, member Member, _ RegisterOptions) (Lease, error) {
	r.mu.Lock()
	r.member = member
	r.mu.Unlock()
	return fakeLease{}, nil
}
func (*recoveryRegistry) UpdateStatus(context.Context, string, MemberStatus) error { return nil }
func (*recoveryRegistry) Unregister(context.Context, string) error                 { return nil }
func (r *recoveryRegistry) List(ctx context.Context) ([]Member, error) {
	r.mu.Lock()
	r.listCalls++
	gate := r.resumeSnapshot
	member := r.member
	r.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return []Member{member}, nil
}
func (r *recoveryRegistry) Watch(ctx context.Context, _ int64) (<-chan Event, <-chan error) {
	watch := &recoveryWatch{events: make(chan Event, 1), errs: make(chan error, 1), stopped: make(chan struct{})}
	r.watches <- watch
	go func() { <-ctx.Done(); close(watch.stopped) }()
	return watch.events, watch.errs
}

func TestRuntimeRecoversAfterWatchDisconnect(t *testing.T) {
	cases := []struct {
		name       string
		disconnect func(*recoveryWatch)
	}{
		{"event stream closes", func(w *recoveryWatch) { close(w.events) }},
		{"error stream closes", func(w *recoveryWatch) { close(w.errs) }},
		{"watch reports error", func(w *recoveryWatch) { w.errs <- errors.New("registry unavailable") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			backend := &recoveryRegistry{watches: make(chan *recoveryWatch, 4)}
			runtime, err := StartRuntime(context.Background(), backend, RuntimeOptions{Member: Member{
				ID: "game-1", ServiceAddr: "127.0.0.1:34580", Services: []string{"GameService"},
			}})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := runtime.Close(ctx); err != nil {
					t.Errorf("Close: %v", err)
				}
			}()
			first := awaitRecoveryWatch(t, backend)
			awaitRegistryCondition(t, func() bool {
				backend.mu.Lock()
				defer backend.mu.Unlock()
				return backend.listCalls >= 1
			}, "initial registry snapshot was not requested")
			backend.mu.Lock()
			resume := make(chan struct{})
			backend.resumeSnapshot = resume
			backend.mu.Unlock()
			tc.disconnect(first)
			awaitRegistryCondition(t, func() bool { return !runtime.AllowNewTraffic() }, "disconnected runtime still allows new traffic")
			close(resume)
			second := awaitRecoveryWatch(t, backend)
			select {
			case <-first.stopped:
			case <-time.After(time.Second):
				t.Fatal("old registry watch context was not canceled")
			}
			awaitRegistryCondition(t, runtime.AllowNewTraffic, "snapshot did not restore active state")
			second.events <- Event{Type: EventPut, Member: Member{ID: "game-1", Status: MemberStatusDraining}}
			awaitRegistryCondition(t, func() bool { return runtime.Status() == MemberStatusDraining }, "replacement watch did not deliver status")
			backend.mu.Lock()
			calls := backend.listCalls
			backend.mu.Unlock()
			if calls < 2 {
				t.Fatalf("List calls = %d, want a new snapshot after disconnect", calls)
			}
		})
	}
}

func awaitRecoveryWatch(t *testing.T, backend *recoveryRegistry) *recoveryWatch {
	t.Helper()
	select {
	case watch := <-backend.watches:
		return watch
	case <-time.After(2 * time.Second):
		t.Fatal("runtime did not establish registry watch")
		return nil
	}
}
func awaitRegistryCondition(t *testing.T, condition func() bool, failure string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal(failure)
}

func TestRuntimeIgnoresBufferedEventsOlderThanSnapshot(t *testing.T) {
	resume := make(chan struct{})
	backend := &recoveryRegistry{watches: make(chan *recoveryWatch, 4), resumeSnapshot: resume}
	observer := &statusObserver{updates: make(chan MemberStatus, 8)}
	runtime, err := StartRuntime(context.Background(), backend, RuntimeOptions{
		Member:   Member{ID: "game-1", ServiceAddr: "127.0.0.1:34580", Services: []string{"GameService"}, Revision: 10},
		Observer: observer,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()
	watch := awaitRecoveryWatch(t, backend)
	// The older event was queued after subscribing but is already represented
	// by the revision-10 snapshot returned by List.
	watch.events <- Event{Type: EventPut, Member: Member{ID: "game-1", Status: MemberStatusDraining, Revision: 9}}
	close(resume)
	watch.events <- Event{Type: EventPut, Member: Member{ID: "game-1", Status: MemberStatusRetired, Revision: 11}}
	var observed []MemberStatus
	for {
		select {
		case status := <-observer.updates:
			observed = append(observed, status)
			if status == MemberStatusRetired {
				if len(observed) != 2 || observed[0] != MemberStatusActive {
					t.Fatalf("status transitions = %v, want [active retired]", observed)
				}
				return
			}
		case <-time.After(time.Second):
			t.Fatal("newer watch event was not applied")
		}
	}
}
