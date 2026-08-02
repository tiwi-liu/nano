package service

import "sync/atomic"

// SessionStatistics tracks authenticated sessions in the current process.
// A gateway exports this separately from the unique-player presence metric.
type SessionStatistics struct {
	authenticated int64
}

var SessionStats = &SessionStatistics{}

func (s *SessionStatistics) Authenticate() {
	atomic.AddInt64(&s.authenticated, 1)
}

func (s *SessionStatistics) ReleaseAuthenticated() {
	atomic.AddInt64(&s.authenticated, -1)
}

func (s *SessionStatistics) Authenticated() int64 {
	return atomic.LoadInt64(&s.authenticated)
}
