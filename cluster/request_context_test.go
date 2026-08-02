package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/lonng/nano/session"
)

func TestNewRequestContextAppliesConfiguredTimeout(t *testing.T) {
	h := &LocalHandler{currentNode: &Node{Options: Options{RequestTimeout: 20 * time.Millisecond}}}
	ctx, cancel := h.newRequestContext(context.Background(), session.New(nil), 1)
	defer cancel()

	select {
	case <-ctx.Done():
		if ctx.Err() != context.DeadlineExceeded {
			t.Fatalf("context error = %v, want deadline exceeded", ctx.Err())
		}
	case <-time.After(time.Second):
		t.Fatal("request context did not time out")
	}
}

func TestNewRequestContextUsesDefaultTimeout(t *testing.T) {
	h := &LocalHandler{currentNode: &Node{}}
	ctx, cancel := h.newRequestContext(context.Background(), session.New(nil), 1)
	defer cancel()

	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("request context has no deadline")
	}
	remaining := time.Until(deadline)
	if remaining <= 0 || remaining > DefaultRequestTimeout {
		t.Fatalf("remaining timeout = %v, want within (0, %v]", remaining, DefaultRequestTimeout)
	}
}
