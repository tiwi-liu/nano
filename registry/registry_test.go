package registry

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestNormalizeMemberAndRoutingPolicy(t *testing.T) {
	member, err := NormalizeMember(Member{ID: "gate-1", ServiceAddr: "127.0.0.1:34570", Services: []string{"GameService", "", "GameService"}})
	if err != nil {
		t.Fatalf("NormalizeMember() error = %v", err)
	}
	if member.Status != MemberStatusActive || len(member.Services) != 1 {
		t.Fatalf("normalized member = %+v", member)
	}
	if !IsRoutable(MemberStatusActive) || IsRoutable(MemberStatusDraining) || IsRoutable(MemberStatusRetired) {
		t.Fatal("only active members should be routable")
	}
	if !IsAddressable(MemberStatusActive) || !IsAddressable(MemberStatusDraining) || IsAddressable(MemberStatusRetired) {
		t.Fatal("active and draining members should be addressable")
	}
	if _, err := NormalizeMember(Member{}); !errors.Is(err, ErrInvalidMember) {
		t.Fatalf("NormalizeMember() error = %v, want ErrInvalidMember", err)
	}
}

func TestViewTracksOnlyRoutableServiceMembers(t *testing.T) {
	view := NewView([]Member{{ID: "game-1", ServiceAddr: "127.0.0.1:34580", Services: []string{"GameService"}, Status: MemberStatusActive}})
	if got := view.RoutableByService("GameService"); len(got) != 1 {
		t.Fatalf("active members = %d, want 1", len(got))
	}
	view.Apply(Event{Type: EventPut, Member: Member{ID: "game-1", ServiceAddr: "127.0.0.1:34580", Services: []string{"GameService"}, Status: MemberStatusDraining}})
	if got := view.RoutableByService("GameService"); len(got) != 0 {
		t.Fatalf("draining members = %d, want 0", len(got))
	}
	view.Apply(Event{Type: EventDelete, Member: Member{ID: "game-1"}})
	if got := view.Members(); len(got) != 0 {
		t.Fatalf("members = %+v, want empty", got)
	}
}

func TestRuntimeReportsStatusWithoutDependingOnMetricsBackend(t *testing.T) {
	backend := newFakeRegistry()
	observer := &statusObserver{updates: make(chan MemberStatus, 2)}
	runtime, err := StartRuntime(context.Background(), backend, RuntimeOptions{
		Member: Member{ID: "game-1", ServiceAddr: "127.0.0.1:34580", Services: []string{"GameService"}},
		TTL:    time.Second, Role: "game", Observer: observer,
	})
	if err != nil {
		t.Fatalf("StartRuntime() error = %v", err)
	}
	defer runtime.Close(context.Background())
	if got := <-observer.updates; got != MemberStatusActive {
		t.Fatalf("initial status = %q, want active", got)
	}
	backend.events <- Event{Type: EventPut, Member: Member{ID: "game-1", Status: MemberStatusDraining}}
	if got := <-observer.updates; got != MemberStatusDraining || runtime.AllowNewTraffic() {
		t.Fatalf("updated status = %q, allow traffic = %v", got, runtime.AllowNewTraffic())
	}
}

func TestRuntimeNotifiesListenersAndCanMarkItselfRetired(t *testing.T) {
	backend := newFakeRegistry()
	runtime, err := StartRuntime(context.Background(), backend, RuntimeOptions{
		Member: Member{ID: "table-1", ServiceAddr: "127.0.0.1:34581", Services: []string{"TexasHoldemService"}},
		TTL:    time.Second,
		Role:   "table",
	})
	if err != nil {
		t.Fatalf("StartRuntime() error = %v", err)
	}
	defer runtime.Close(context.Background())

	updates := make(chan MemberStatus, 2)
	runtime.AddStatusListener(func(status MemberStatus) { updates <- status })
	if got := <-updates; got != MemberStatusActive {
		t.Fatalf("initial listener status = %q, want active", got)
	}
	backend.events <- Event{Type: EventPut, Member: Member{ID: "table-1", Status: MemberStatusDraining}}
	if got := <-updates; got != MemberStatusDraining {
		t.Fatalf("listener status = %q, want draining", got)
	}
	if err := runtime.MarkRetired(context.Background()); err != nil {
		t.Fatalf("MarkRetired() error = %v", err)
	}
	if backend.updatedID != "table-1" || backend.updatedStatus != MemberStatusRetired {
		t.Fatalf("status update = (%q, %q), want (table-1, retired)", backend.updatedID, backend.updatedStatus)
	}
}

type statusObserver struct{ updates chan MemberStatus }

func (o *statusObserver) RegistryStatusChanged(_ string, _ string, status MemberStatus) {
	o.updates <- status
}

type fakeRegistry struct {
	mu            sync.Mutex
	events        chan Event
	errs          chan error
	member        Member
	updatedID     string
	updatedStatus MemberStatus
}

func newFakeRegistry() *fakeRegistry {
	return &fakeRegistry{events: make(chan Event, 2), errs: make(chan error, 1)}
}
func (f *fakeRegistry) Register(_ context.Context, member Member, _ RegisterOptions) (Lease, error) {
	f.mu.Lock()
	f.member = member
	f.mu.Unlock()
	return fakeLease{}, nil
}
func (f *fakeRegistry) UpdateStatus(_ context.Context, id string, status MemberStatus) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updatedID = id
	f.updatedStatus = status
	return nil
}
func (f *fakeRegistry) Unregister(context.Context, string) error { return nil }
func (f *fakeRegistry) List(context.Context) ([]Member, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return []Member{f.member}, nil
}
func (f *fakeRegistry) Watch(context.Context, int64) (<-chan Event, <-chan error) {
	return f.events, f.errs
}

type fakeLease struct{}

func (fakeLease) KeepAlive(context.Context) error { return nil }
func (fakeLease) Close(context.Context) error     { return nil }
