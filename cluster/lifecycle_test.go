package cluster

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/lonng/nano/component"
	"github.com/lonng/nano/session"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestHandleRemovesLocalSessionOnDisconnect(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	n := &Node{
		Options: Options{
			Components: &component.Components{},
			RPCTimeout: time.Millisecond,
		},
		ServiceAddr: "127.0.0.1:0",
		sessions:    map[int64]*session.Session{},
	}
	n.cluster = newCluster(n)
	n.handler = NewHandler(n, nil)

	done := make(chan struct{})
	go func() {
		n.handler.handle(serverConn)
		close(done)
	}()

	waitFor(t, time.Second, func() bool {
		n.mu.RLock()
		defer n.mu.RUnlock()
		return len(n.sessions) == 1
	})

	if err := clientConn.Close(); err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handler did not exit after connection close")
	}

	n.mu.RLock()
	defer n.mu.RUnlock()
	if len(n.sessions) != 0 {
		t.Fatalf("sessions = %d, want 0", len(n.sessions))
	}
}

func TestClusterAuthUnaryInterceptor(t *testing.T) {
	n := &Node{Options: Options{ClusterAuthToken: "secret"}}
	interceptor := n.authUnaryInterceptor()
	info := &grpc.UnaryServerInfo{FullMethod: "/clusterpb.Member/HandleRequest"}
	handler := func(context.Context, interface{}) (interface{}, error) {
		return "ok", nil
	}

	if _, err := interceptor(context.Background(), nil, info, handler); err == nil {
		t.Fatal("missing token should be rejected")
	}

	bad := metadata.NewIncomingContext(context.Background(), metadata.Pairs(clusterAuthMetadataKey, "bad"))
	if _, err := interceptor(bad, nil, info, handler); err == nil {
		t.Fatal("bad token should be rejected")
	}

	good := metadata.NewIncomingContext(context.Background(), metadata.Pairs(clusterAuthMetadataKey, "secret"))
	got, err := interceptor(good, nil, info, handler)
	if err != nil {
		t.Fatal(err)
	}
	if got != "ok" {
		t.Fatalf("handler result = %v, want ok", got)
	}
}

func waitFor(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not satisfied before timeout")
}
