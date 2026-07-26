package cluster

import (
	"context"
	"testing"

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
}
