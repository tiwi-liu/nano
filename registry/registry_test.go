package registry

import (
	"context"
	"errors"
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

type statusObserver struct{ updates chan MemberStatus }

func (o *statusObserver) RegistryStatusChanged(_ string, _ string, status MemberStatus) {
	o.updates <- status
}

type fakeRegistry struct {
	events chan Event
	errs   chan error
}

func newFakeRegistry() *fakeRegistry {
	return &fakeRegistry{events: make(chan Event, 2), errs: make(chan error, 1)}
}
func (f *fakeRegistry) Register(context.Context, Member, RegisterOptions) (Lease, error) {
	return fakeLease{}, nil
}
func (f *fakeRegistry) UpdateStatus(context.Context, string, MemberStatus) error { return nil }
func (f *fakeRegistry) Unregister(context.Context, string) error                 { return nil }
func (f *fakeRegistry) List(context.Context) ([]Member, error) {
	return []Member{{ID: "game-1", Status: MemberStatusActive}}, nil
}
func (f *fakeRegistry) Watch(context.Context, int64) (<-chan Event, <-chan error) {
	return f.events, f.errs
}

type fakeLease struct{}

func (fakeLease) KeepAlive(context.Context) error { return nil }
func (fakeLease) Close(context.Context) error     { return nil }
