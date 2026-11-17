package scheduler

import (
	"testing"
	"time"
)

func TestConcurrentTimerCreationAndStop(t *testing.T) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			tm := NewTimer(time.Nanosecond, func() {})
			tm.Stop()
		}
	}()
	for i := 0; i < 1000; i++ {
		cron()
	}
	<-done
	cron()
}

type alwaysReadyCondition struct{}

func (alwaysReadyCondition) Check(time.Time) bool { return true }

func TestStoppedConditionTimerIsRemoved(t *testing.T) {
	calls := 0
	timer := NewCondTimer(alwaysReadyCondition{}, func() { calls++ })
	cron()
	if calls != 1 {
		t.Fatalf("condition callback calls = %d", calls)
	}
	timer.Stop()
	timer.Stop()
	cron()
	if calls != 1 {
		t.Fatal("stopped condition timer fired")
	}
	if _, ok := timerManager.timers[timer.ID()]; ok {
		t.Fatal("stopped condition timer retained")
	}
}
