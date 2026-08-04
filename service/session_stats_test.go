package service

import "testing"

func TestSessionStatisticsCountsUniqueOnlinePlayers(t *testing.T) {
	stats := &SessionStatistics{}

	stats.AuthenticateUID(100)
	stats.AuthenticateUID(100)
	stats.AuthenticateUID(200)
	if got := stats.Authenticated(); got != 3 {
		t.Fatalf("authenticated sessions = %d, want 3", got)
	}
	if got := stats.OnlinePlayers(); got != 2 {
		t.Fatalf("online players = %d, want 2", got)
	}

	stats.ReleaseAuthenticatedUID(100)
	if got := stats.OnlinePlayers(); got != 2 {
		t.Fatalf("online players after one duplicate session closes = %d, want 2", got)
	}
	stats.ReleaseAuthenticatedUID(100)
	if got := stats.OnlinePlayers(); got != 1 {
		t.Fatalf("online players after final UID 100 session closes = %d, want 1", got)
	}
	stats.ReleaseAuthenticatedUID(200)
	if got := stats.OnlinePlayers(); got != 0 {
		t.Fatalf("online players after all sessions close = %d, want 0", got)
	}
}
