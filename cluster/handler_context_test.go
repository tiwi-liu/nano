package cluster

import (
	"context"
	"testing"
	"time"
)

type requestContextKey struct{}

func TestNewRequestContextPreservesParentValuesWithoutParentCancellation(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.WithValue(context.Background(), requestContextKey{}, "trace-context"))
	handler := &LocalHandler{currentNode: &Node{Options: Options{RequestTimeout: time.Second}}}

	requestContext, cancel := handler.newRequestContext(parent, nil, 1)
	defer cancel()
	cancelParent()

	if got := requestContext.Value(requestContextKey{}); got != "trace-context" {
		t.Fatalf("parent value = %v, want trace-context", got)
	}
	if err := requestContext.Err(); err != nil {
		t.Fatalf("request context was cancelled with completed gRPC call: %v", err)
	}
}
