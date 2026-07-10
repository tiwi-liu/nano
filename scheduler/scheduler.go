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
)

func try(f func()) {
	defer func() {
		if err := recover(); err != nil {
			log.Println(fmt.Sprintf("Handle message panic: %+v\n%s", err, debug.Stack()))
		}
	}()
	f()
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
	chTasks <- task
}

// TryPushTask tries to enqueue task without blocking. It returns false if the queue is full.
func TryPushTask(task Task) bool {
	select {
	case chTasks <- task:
		return true
	default:
		return false
	}
}
