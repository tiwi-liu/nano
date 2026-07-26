package registry

import (
	"context"
	"fmt"
	"strings"
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
	if members, err := r.registry.List(ctx); err == nil {
		r.applySnapshot(members)
	}
	events, errs := r.registry.Watch(ctx, 0)
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-events:
			if !ok {
				return
			}
			r.applyEvent(event)
		case _, ok := <-errs:
			if ok {
				return
			}
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
}
func ServiceCSV(services []string) string { return strings.Join(compactStrings(services), ",") }
