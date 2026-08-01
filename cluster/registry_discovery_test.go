package cluster

import (
	"context"
	"testing"

	"github.com/lonng/nano/pkg/errcode"
	"github.com/lonng/nano/registry"
)

type fakeDiscoveryRegistry struct {
	members []registry.Member
	events  chan registry.Event
	errs    chan error
}

func (f *fakeDiscoveryRegistry) Register(context.Context, registry.Member, registry.RegisterOptions) (registry.Lease, error) {
	return nil, nil
}
func (f *fakeDiscoveryRegistry) UpdateStatus(context.Context, string, registry.MemberStatus) error {
	return nil
}
func (f *fakeDiscoveryRegistry) Unregister(context.Context, string) error { return nil }
func (f *fakeDiscoveryRegistry) List(context.Context) ([]registry.Member, error) {
	return append([]registry.Member(nil), f.members...), nil
}
func (f *fakeDiscoveryRegistry) Watch(context.Context, int64) (<-chan registry.Event, <-chan error) {
	return f.events, f.errs
}

func TestRegistryDiscoveryRemovesNonRoutableMembers(t *testing.T) {
	node := &Node{Options: Options{MemberAddr: "self:1"}}
	node.cluster = newCluster(node)
	node.handler = NewHandler(node, nil)

	active := registry.Member{ID: "table-1", Label: "node", ServiceAddr: "table:34581", Services: []string{"TableService"}, Status: registry.MemberStatusActive}
	node.applyRegistryEvent(registry.Event{Type: registry.EventPut, Member: active})
	if got := node.Handler().RemoteService(); len(got) != 1 || got[0] != "TableService" {
		t.Fatalf("remote services after active = %v, want [TableService]", got)
	}

	active.Status = registry.MemberStatusDraining
	node.applyRegistryEvent(registry.Event{Type: registry.EventPut, Member: active})
	if got := node.Handler().RemoteService(); len(got) != 0 {
		t.Fatalf("remote services after draining = %v, want empty", got)
	}
}

func TestRegistryDiscoverySnapshotKeepsOnlyActiveRemoteMembers(t *testing.T) {
	node := &Node{Options: Options{MemberAddr: "self:1"}}
	node.cluster = newCluster(node)
	node.handler = NewHandler(node, nil)

	node.applyRegistryMembers([]registry.Member{
		{ID: "self", ServiceAddr: "self:1", Services: []string{"SelfService"}, Status: registry.MemberStatusActive},
		{ID: "game", ServiceAddr: "game:1", Services: []string{"GameService"}, Status: registry.MemberStatusActive},
		{ID: "table", ServiceAddr: "table:1", Services: []string{"TableService"}, Status: registry.MemberStatusDraining},
	})
	if got := node.Handler().RemoteService(); len(got) != 1 || got[0] != "GameService" {
		t.Fatalf("remote services = %v, want [GameService]", got)
	}
	instance, ok := node.handler.findAddressableInstance("table")
	if !ok || instance.ServiceAddr != "table:1" || instance.Status != registry.MemberStatusDraining {
		t.Fatalf("draining instance = %+v, %v", instance, ok)
	}
	if _, ok := node.handler.selectInstanceByKey("TableService", "room-7"); ok {
		t.Fatal("draining instance must not be selected for new placement")
	}
}

func TestRegistryDiscoverySelectsActiveInstanceDeterministically(t *testing.T) {
	node := &Node{Options: Options{MemberAddr: "self:1"}}
	node.cluster = newCluster(node)
	node.handler = NewHandler(node, nil)
	node.applyRegistryMembers([]registry.Member{
		{ID: "texas-a", ServiceAddr: "table-a:1", Services: []string{"TexasHoldemService"}, Status: registry.MemberStatusActive},
		{ID: "texas-b", ServiceAddr: "table-b:1", Services: []string{"TexasHoldemService"}, Status: registry.MemberStatusActive},
		{ID: "texas-c", ServiceAddr: "table-c:1", Services: []string{"TexasHoldemService"}, Status: registry.MemberStatusDraining},
	})
	first, ok := node.handler.selectInstanceByKey("TexasHoldemService", "table-42")
	if !ok {
		t.Fatal("SelectInstanceByKey() did not find active instance")
	}
	for i := 0; i < 20; i++ {
		next, ok := node.handler.selectInstanceByKey("TexasHoldemService", "table-42")
		if !ok || next.ID != first.ID {
			t.Fatalf("selection changed from %q to %+v", first.ID, next)
		}
	}
	if first.ID == "texas-c" {
		t.Fatal("draining instance selected for placement")
	}
}

func TestRegistryDiscoveryRetiredInstanceIsNotAddressable(t *testing.T) {
	node := &Node{Options: Options{MemberAddr: "self:1"}}
	node.cluster = newCluster(node)
	node.handler = NewHandler(node, nil)

	member := registry.Member{ID: "texas-a", ServiceAddr: "table-a:1", Services: []string{"TexasHoldemService"}, Status: registry.MemberStatusDraining}
	node.applyRegistryEvent(registry.Event{Type: registry.EventPut, Member: member})
	if _, ok := node.handler.findAddressableInstance(member.ID); !ok {
		t.Fatal("draining instance should remain addressable")
	}

	member.Status = registry.MemberStatusRetired
	node.applyRegistryEvent(registry.Event{Type: registry.EventPut, Member: member})
	if _, ok := node.handler.findAddressableInstance(member.ID); ok {
		t.Fatal("retired instance must be removed from the addressable directory")
	}
}

func TestCallToRejectsServiceNotExposedByTargetInstance(t *testing.T) {
	node := &Node{Options: Options{MemberAddr: "self:1"}}
	node.cluster = newCluster(node)
	node.handler = NewHandler(node, nil)
	node.applyRegistryMembers([]registry.Member{
		{ID: "texas-a", ServiceAddr: "table-a:1", Services: []string{"TexasHoldemService"}, Status: registry.MemberStatusDraining},
	})

	err := node.handler.CallTo(context.Background(), 1, "texas-a", "MahjongService.Join", struct{}{}, &struct{}{})
	callErr, ok := err.(*InternalCallError)
	if !ok || callErr.Code != errcode.CodeServiceNotFound {
		t.Fatalf("CallTo() error = %v, want service-not-found", err)
	}
}
