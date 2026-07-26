package registry

import "sync"

type View struct {
	mu      sync.RWMutex
	members map[string]Member
}

func NewView(initial []Member) *View {
	v := &View{members: make(map[string]Member, len(initial))}
	for _, m := range initial {
		v.Apply(Event{Type: EventPut, Member: m})
	}
	return v
}
func (v *View) Apply(event Event) {
	if event.Member.ID == "" {
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if event.Type == EventPut {
		v.members[event.Member.ID] = event.Member
	} else if event.Type == EventDelete {
		delete(v.members, event.Member.ID)
	}
}
func (v *View) Members() []Member {
	v.mu.RLock()
	defer v.mu.RUnlock()
	result := make([]Member, 0, len(v.members))
	for _, m := range v.members {
		result = append(result, m)
	}
	return result
}
func (v *View) RoutableByService(service string) []Member {
	v.mu.RLock()
	defer v.mu.RUnlock()
	result := make([]Member, 0)
	for _, m := range v.members {
		if IsRoutable(m.Status) && memberHasService(m, service) {
			result = append(result, m)
		}
	}
	return result
}
func memberHasService(member Member, service string) bool {
	if service == "" {
		return false
	}
	for _, item := range member.Services {
		if item == service {
			return true
		}
	}
	return false
}
