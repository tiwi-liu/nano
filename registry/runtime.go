package registry

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Observer keeps registry lifecycle independent from metrics and logging backends.
type Observer interface {
	RegistryStatusChanged(memberID, role string, status MemberStatus)
}
type RuntimeOptions struct {
	Member   Member
	TTL      time.Duration
	Role     string
	Observer Observer
}
type Runtime struct {
	registry       Registry
	memberID, role string
	lease          Lease
	observer       Observer
	status         atomic.Value
	listenersMu    sync.RWMutex
	listeners      []func(MemberStatus)
	cancel         context.CancelFunc
	done           chan struct{}
}

func StartRuntime(ctx context.Context, backend Registry, options RuntimeOptions) (*Runtime, error) {
	if backend == nil {
		return nil, fmt.Errorf("registry is nil")
	}
	member, err := NormalizeMember(options.Member)
	if err != nil {
		return nil, err
	}
	lease, err := backend.Register(ctx, member, RegisterOptions{TTL: options.TTL})
	if err != nil {
		return nil, err
	}
	watchCtx, cancel := context.WithCancel(context.Background())
	r := &Runtime{registry: backend, memberID: member.ID, role: options.Role, lease: lease, observer: options.Observer, cancel: cancel, done: make(chan struct{})}
	r.setStatus(member.Status)
	go r.watchSelf(watchCtx)
	return r, nil
}
func (r *Runtime) Status() MemberStatus {
	status, _ := r.status.Load().(MemberStatus)
	if status == "" {
		return MemberStatusRetired
	}
	return status
}
func (r *Runtime) AllowNewTraffic() bool { return IsRoutable(r.Status()) }
func (r *Runtime) AddStatusListener(listener func(MemberStatus)) {
	if r == nil || listener == nil {
		return
	}
	r.listenersMu.Lock()
	r.listeners = append(r.listeners, listener)
	r.listenersMu.Unlock()
	listener(r.Status())
}
func (r *Runtime) MarkRetired(ctx context.Context) error {
	if r == nil || r.registry == nil || r.memberID == "" {
		return ErrInvalidMember
	}
	return r.registry.UpdateStatus(ctx, r.memberID, MemberStatusRetired)
}
func (r *Runtime) Close(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.cancel()
	select {
	case <-r.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	return r.lease.Close(ctx)
}
func (r *Runtime) watchSelf(ctx context.Context) {
	defer close(r.done)
	for ctx.Err() == nil {
		r.watchAttempt(ctx)
		if ctx.Err() != nil {
			return
		}
		r.setStatus(MemberStatusRetired)
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (r *Runtime) watchAttempt(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	// Subscribe before taking the snapshot so changes during List are retained.
	events, errs := r.registry.Watch(ctx, 0)
	listCtx, stopList := context.WithTimeout(ctx, 3*time.Second)
	members, err := r.registry.List(listCtx)
	stopList()
	if err != nil {
		return
	}
	var revision int64
	for _, member := range members {
		if member.ID == r.memberID {
			revision = member.Revision
			break
		}
	}
	r.applySnapshot(members)
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-events:
			if !ok {
				return
			}
			if event.Member.ID != r.memberID {
				continue
			}
			if event.Member.Revision > 0 && event.Member.Revision <= revision {
				continue
			}
			if event.Member.Revision > 0 {
				revision = event.Member.Revision
			}
			r.applyEvent(event)
		case <-errs:
			return
		}
	}
}

func (r *Runtime) applySnapshot(members []Member) {
	for _, m := range members {
		if m.ID == r.memberID {
			r.setStatus(m.Status)
			return
		}
	}
	r.setStatus(MemberStatusRetired)
}
func (r *Runtime) applyEvent(event Event) {
	if event.Member.ID != r.memberID {
		return
	}
	if event.Type == EventDelete {
		r.setStatus(MemberStatusRetired)
	} else {
		r.setStatus(event.Member.Status)
	}
}
func (r *Runtime) setStatus(status MemberStatus) {
	if !ValidStatus(status) {
		status = MemberStatusRetired
	}
	if current, _ := r.status.Load().(MemberStatus); current == status {
		return
	}
	r.status.Store(status)
	if r.observer != nil {
		r.observer.RegistryStatusChanged(r.memberID, r.role, status)
	}
	r.listenersMu.RLock()
	listeners := append([]func(MemberStatus){}, r.listeners...)
	r.listenersMu.RUnlock()
	for _, listener := range listeners {
		listener(status)
	}
}
func ServiceCSV(services []string) string { return strings.Join(compactStrings(services), ",") }
