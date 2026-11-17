package scheduler

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCloseTasksRunWhenBusinessQueueIsFull(t *testing.T) {
	previous := chTasks
	chTasks = make(chan Task, 1)
	chTasks <- func() {}
	defer func() { chTasks = previous }()
	var calls atomic.Int32
	queued := make(chan struct{})
	executed := make(chan struct{})
	go func() {
		PushCloseTask(func() { calls.Add(1) })
		PushCloseTask(func() { panic("callback failure") })
		PushCloseTask(func() { calls.Add(1); close(executed) })
		close(queued)
	}()
	awaitCloseTaskSignal(t, queued, "lifecycle enqueue blocked on business queue")
	awaitCloseTaskSignal(t, executed, "lifecycle callbacks were not executed")
	waitCloseTasks()
	if calls.Load() != 2 {
		t.Fatalf("lifecycle calls = %v", calls.Load())
	}
	waitCloseTasks()
	if calls.Load() != 2 {
		t.Fatal("lifecycle callbacks ran more than once")
	}
}

func TestCloseTasksRemainAvailableAfterSchedulerShutdown(t *testing.T) {
	previousDie, previousExit, previousTasks := chDie, chExit, chTasks
	previousStarted, previousClosed, previousWorkers := started, closed, workerCnt
	chDie, chExit, chTasks = make(chan struct{}), make(chan struct{}), make(chan Task, 1)
	atomic.StoreInt32(&started, 0)
	atomic.StoreInt32(&closed, 0)
	atomic.StoreInt32(&workerCnt, 1)
	defer func() {
		chDie, chExit, chTasks = previousDie, previousExit, previousTasks
		atomic.StoreInt32(&started, previousStarted)
		atomic.StoreInt32(&closed, previousClosed)
		atomic.StoreInt32(&workerCnt, previousWorkers)
	}()
	defer Close()
	go Sched()
	workerReady := make(chan struct{})
	PushTask(func() { close(workerReady) })
	awaitCloseTaskSignal(t, workerReady, "business worker did not start")
	Close()
	// A connection's read goroutine can finish after the business scheduler.
	// Its lifecycle callback must still run, including callbacks it queues.
	nestedDone := make(chan struct{})
	PushCloseTask(func() { PushCloseTask(func() { close(nestedDone) }) })
	awaitCloseTaskSignal(t, nestedDone, "late or recursively queued lifecycle callback was lost")
	waitCloseTasks()
}

func TestCloseTasksRespectWorkerLimitAcrossConcurrentProducers(t *testing.T) {
	previous := atomic.SwapInt32(&workerCnt, 2)
	defer atomic.StoreInt32(&workerCnt, previous)
	const count = 100
	var active, maximum int32
	var callbacks sync.WaitGroup
	callbacks.Add(count)
	var producers sync.WaitGroup
	producers.Add(count)
	for i := 0; i < count; i++ {
		go func() {
			defer producers.Done()
			PushCloseTask(func() {
				concurrency := atomic.AddInt32(&active, 1)
				for {
					observed := atomic.LoadInt32(&maximum)
					if concurrency <= observed || atomic.CompareAndSwapInt32(&maximum, observed, concurrency) {
						break
					}
				}
				time.Sleep(time.Microsecond)
				atomic.AddInt32(&active, -1)
				callbacks.Done()
			})
		}()
	}
	producers.Wait()
	done := make(chan struct{})
	go func() { callbacks.Wait(); close(done) }()
	awaitCloseTaskSignal(t, done, "lifecycle callbacks did not finish")
	waitCloseTasks()
	if got := atomic.LoadInt32(&maximum); got < 1 || got > 2 {
		t.Fatalf("concurrent lifecycle callbacks = %d, want at most 2", got)
	}
}

func awaitCloseTaskSignal(t *testing.T, signal <-chan struct{}, failure string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatal(failure)
	}
}
