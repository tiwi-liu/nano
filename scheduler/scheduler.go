// Copyright (c) nano Authors. All Rights Reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package scheduler

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lonng/nano/internal/env"
	"github.com/lonng/nano/internal/log"
)

const (
	messageQueueBacklog = 1 << 10
	sessionCloseBacklog = 1 << 8
)

// LocalScheduler schedules task to a customized goroutine
type LocalScheduler interface {
	Schedule(Task)
}

type Task func()

type Hook func()

var (
	chDie   = make(chan struct{})
	chExit  = make(chan struct{})
	chTasks = make(chan Task, 1<<8)
	started int32
	closed  int32

	workerCnt int32 // must be configured before Sched

	submittedTotal int64
	enqueuedTotal  int64
	rejectedTotal  int64
	startedTotal   int64
	completedTotal int64
	waitNanosTotal int64
	runNanosTotal  int64
)

type StatsSnapshot struct {
	QueueLen       int
	QueueCap       int
	SubmittedTotal int64
	EnqueuedTotal  int64
	RejectedTotal  int64
	StartedTotal   int64
	CompletedTotal int64
	WaitNanosTotal int64
	RunNanosTotal  int64
}

func try(f func()) {
	defer func() {
		if err := recover(); err != nil {
			log.Println(fmt.Sprintf("Handle message panic: %+v\n%s", err, debug.Stack()))
		}
	}()
	f()
}

func instrument(task Task) Task {
	if task == nil {
		return nil
	}
	enqueuedAt := time.Now()
	return func() {
		atomic.AddInt64(&startedTotal, 1)
		atomic.AddInt64(&waitNanosTotal, time.Since(enqueuedAt).Nanoseconds())
		startedAt := time.Now()
		defer func() {
			atomic.AddInt64(&runNanosTotal, time.Since(startedAt).Nanoseconds())
			atomic.AddInt64(&completedTotal, 1)
		}()
		task()
	}
}

func Stats() StatsSnapshot {
	return StatsSnapshot{
		QueueLen:       len(chTasks),
		QueueCap:       cap(chTasks),
		SubmittedTotal: atomic.LoadInt64(&submittedTotal),
		EnqueuedTotal:  atomic.LoadInt64(&enqueuedTotal),
		RejectedTotal:  atomic.LoadInt64(&rejectedTotal),
		StartedTotal:   atomic.LoadInt64(&startedTotal),
		CompletedTotal: atomic.LoadInt64(&completedTotal),
		WaitNanosTotal: atomic.LoadInt64(&waitNanosTotal),
		RunNanosTotal:  atomic.LoadInt64(&runNanosTotal),
	}
}

// Configure sets scheduler worker count and task backlog.
// It must be called before Sched() starts, otherwise it has no effect.
func Configure(workers, backlog int) {
	if atomic.LoadInt32(&started) != 0 {
		return
	}
	if workers > 0 {
		atomic.StoreInt32(&workerCnt, int32(workers))
	}
	if backlog > 0 {
		chTasks = make(chan Task, backlog)
	}
}

func Sched() {
	if atomic.AddInt32(&started, 1) != 1 {
		return
	}

	wc := int(atomic.LoadInt32(&workerCnt))
	if wc <= 0 {
		wc = runtime.GOMAXPROCS(0)
		if wc <= 0 {
			wc = 1
		}
	}

	var wg sync.WaitGroup

	// timer loop (single goroutine)
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(env.TimerPrecision)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				cron()
			case <-chDie:
				return
			}
		}
	}()

	// task workers
	for i := 0; i < wc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case f := <-chTasks:
					if f != nil {
						try(f)
					}
				case <-chDie:
					return
				}
			}
		}()
	}

	wg.Wait()
	close(chExit)
}

func Close() {
	if atomic.AddInt32(&closed, 1) != 1 {
		return
	}
	close(chDie)
	<-chExit
	log.Println("Scheduler stopped")
}

func PushTask(task Task) {
	atomic.AddInt64(&submittedTotal, 1)
	chTasks <- instrument(task)
	atomic.AddInt64(&enqueuedTotal, 1)
}

// TryPushTask tries to enqueue task without blocking. It returns false if the queue is full.
func TryPushTask(task Task) bool {
	atomic.AddInt64(&submittedTotal, 1)
	select {
	case chTasks <- instrument(task):
		atomic.AddInt64(&enqueuedTotal, 1)
		return true
	default:
		atomic.AddInt64(&rejectedTotal, 1)
		return false
	}
}
