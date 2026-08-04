package service

import (
	"sync"
	"sync/atomic"
)

// SessionStatistics tracks authenticated sessions in the current process.
// A gateway exports this separately from the unique-player presence metric.
type SessionStatistics struct {
	authenticated int64
	onlinePlayers int64
	mu            sync.Mutex
	uidSessions   map[int64]int64
}

var SessionStats = &SessionStatistics{}

func (s *SessionStatistics) Authenticate() {
	atomic.AddInt64(&s.authenticated, 1)
}

// AuthenticateUID tracks both the authenticated session and the process-local
// unique player count. Multiple sessions bound to one UID count as one player.
func (s *SessionStatistics) AuthenticateUID(uid int64) {
	s.Authenticate()
	if uid < 1 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.uidSessions == nil {
		s.uidSessions = make(map[int64]int64)
	}
	if s.uidSessions[uid] == 0 {
		atomic.AddInt64(&s.onlinePlayers, 1)
	}
	s.uidSessions[uid]++
}

func (s *SessionStatistics) ReleaseAuthenticated() {
	atomic.AddInt64(&s.authenticated, -1)
}

// ReleaseAuthenticatedUID releases one UID-bound session. The player remains
// online until the final session for that UID closes.
func (s *SessionStatistics) ReleaseAuthenticatedUID(uid int64) {
	if uid > 0 {
		s.mu.Lock()
		if count := s.uidSessions[uid]; count > 1 {
			s.uidSessions[uid] = count - 1
		} else if count == 1 {
			delete(s.uidSessions, uid)
			atomic.AddInt64(&s.onlinePlayers, -1)
		}
		s.mu.Unlock()
	}
	s.ReleaseAuthenticated()
}

func (s *SessionStatistics) Authenticated() int64 {
	return atomic.LoadInt64(&s.authenticated)
}

func (s *SessionStatistics) OnlinePlayers() int64 {
	return atomic.LoadInt64(&s.onlinePlayers)
}
