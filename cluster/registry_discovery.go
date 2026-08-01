package cluster

import (
	"context"
	"time"

	"github.com/lonng/nano/cluster/clusterpb"
	"github.com/lonng/nano/internal/env"
	"github.com/lonng/nano/internal/log"
	"github.com/lonng/nano/registry"
)

func (n *Node) startRegistryDiscovery() error {
	ctx, cancel := context.WithTimeout(context.Background(), n.rpcTimeout())
	members, err := n.ServiceRegistry.List(ctx)
	cancel()
	if err != nil {
		return err
	}
	n.applyRegistryMembers(members)

	watchCtx, stop := context.WithCancel(context.Background())
	n.registryExit = stop
	go n.watchRegistry(watchCtx)
	return nil
}

func (n *Node) watchRegistry(ctx context.Context) {
	resync := time.NewTicker(env.Heartbeat)
	defer resync.Stop()

	for {
		events, errs := n.ServiceRegistry.Watch(ctx, 0)
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-events:
				if !ok {
					events = nil
					continue
				}
				n.applyRegistryEvent(event)
			case err, ok := <-errs:
				if ok && err != nil {
					log.Println("Registry watch error", err)
				}
				events, errs = nil, nil
			case <-resync.C:
				n.resyncRegistry(ctx)
			}
			if events == nil && errs == nil {
				break
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(n.RetryInterval):
			n.resyncRegistry(ctx)
		}
	}
}

func (n *Node) resyncRegistry(parent context.Context) {
	ctx, cancel := context.WithTimeout(parent, n.rpcTimeout())
	members, err := n.ServiceRegistry.List(ctx)
	cancel()
	if err != nil {
		log.Println("Registry resync error", err)
		return
	}
	n.applyRegistryMembers(members)
}

func (n *Node) applyRegistryEvent(event registry.Event) {
	n.handler.applyRegistryInstance(event, n.memberAddr())
	member := registryMemberInfo(event.Member, n.memberAddr())
	if event.Type == registry.EventDelete || member == nil {
		if event.Member.ServiceAddr != "" {
			n.handler.delMember(event.Member.ServiceAddr)
			n.cluster.delMember(event.Member.ServiceAddr)
		} else {
			n.resyncRegistry(context.Background())
		}
		return
	}
	n.handler.addRemoteService(member)
	n.cluster.addMember(member)
}

func (n *Node) applyRegistryMembers(members []registry.Member) {
	n.handler.syncRegistryInstances(members, n.memberAddr())
	infos := make([]*clusterpb.MemberInfo, 0, len(members))
	for _, member := range members {
		info := registryMemberInfo(member, n.memberAddr())
		if info != nil {
			infos = append(infos, info)
		}
	}
	n.handler.syncRemoteMembers(infos)
	n.cluster.syncMembers(infos)
}

func registryMemberInfo(member registry.Member, selfAddr string) *clusterpb.MemberInfo {
	if !registry.IsRoutable(member.Status) || member.ServiceAddr == "" || member.ServiceAddr == selfAddr {
		return nil
	}
	return &clusterpb.MemberInfo{
		Label:       member.Label,
		ServiceAddr: member.ServiceAddr,
		Services:    append([]string(nil), member.Services...),
	}
}
