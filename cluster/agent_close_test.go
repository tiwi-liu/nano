package cluster

import (
	"context"
	"io"
	"net"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lonng/nano/scheduler"
	"github.com/lonng/nano/session"
)

func TestAgentCloseWithFullBusinessQueue(t *testing.T) {
	// Keep process-global scheduler state isolated from the remaining tests.
	if os.Getenv("NANO_TEST_FULL_QUEUE") != "1" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestAgentCloseWithFullBusinessQueue$")
		command.Env = append(os.Environ(), "NANO_TEST_FULL_QUEUE=1")
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("close with saturated queue: %v\n%s", err, output)
		}
		return
	}
	for scheduler.TryPushTask(func() {}) {
	}
	server, client := net.Pipe()
	defer client.Close()
	agent := newAgent(server, nil, nil, time.Second)
	if err := agent.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("read after close: %v", err)
	}
}

func TestAgentConcurrentClose(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()
	agent := newAgent(server, nil, nil, time.Second)
	var successes atomic.Int32
	var workers sync.WaitGroup
	for i := 0; i < 100; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			if agent.Close() == nil {
				successes.Add(1)
			}
		}()
	}
	workers.Wait()
	if successes.Load() != 1 {
		t.Fatalf("successful closes = %d, want 1", successes.Load())
	}
	agent.setStatus(statusWorking)
	if agent.status() != statusClosed {
		t.Fatal("closed agent returned to working state")
	}
}

func TestHandshakeTimeoutClosesSocketBeforeCleanupCallback(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()
	release := make(chan struct{})
	cleanupStarted := make(chan struct{})
	finished := make(chan struct{})
	node := &Node{Options: Options{RequestTimeout: 20 * time.Millisecond, SessionClosedCallback: func(*session.Session) { close(cleanupStarted); <-release }}, sessions: make(map[int64]*session.Session)}
	node.cluster = newCluster(node)
	node.handler = NewHandler(node, nil)
	go func() { node.handler.handle(server); close(finished) }()
	defer func() { close(release); <-finished }()
	select {
	case <-cleanupStarted:
	case <-time.After(time.Second):
		t.Fatal("handshake did not time out")
	}
	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := client.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("socket stayed open during cleanup: %v", err)
	}
}
