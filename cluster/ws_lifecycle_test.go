package cluster

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/lonng/nano/component"
	"github.com/lonng/nano/internal/codec"
	"github.com/lonng/nano/internal/packet"
	"github.com/lonng/nano/session"
)

func openLifecycleWebsocket(t *testing.T) (*Node, *websocket.Conn) {
	t.Helper()
	node := &Node{Options: Options{Components: &component.Components{}, RequestTimeout: 250 * time.Millisecond},
		sessions: make(map[int64]*session.Session)}
	node.cluster = newCluster(node)
	node.handler = NewHandler(node, nil)
	cache()
	upgrader := websocket.Upgrader{}
	accepted := make(chan *websocket.Conn, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		accepted <- conn
		node.handler.handleWS(conn)
	}))
	client, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	serverConn := <-accepted
	t.Cleanup(func() {
		_ = client.Close()
		_ = serverConn.Close()
		server.Close()
		waitFor(t, time.Second, func() bool {
			node.mu.RLock()
			defer node.mu.RUnlock()
			return len(node.sessions) == 0
		})
	})
	return node, client
}

func TestWebsocketTracksConnectionBeforeFirstFrame(t *testing.T) {
	node, _ := openLifecycleWebsocket(t)
	waitFor(t, 200*time.Millisecond, func() bool {
		node.mu.RLock()
		defer node.mu.RUnlock()
		return len(node.sessions) == 1
	})
}

func TestWebsocketClosesWhenFirstFrameNeverArrives(t *testing.T) {
	_, client := openLifecycleWebsocket(t)
	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	_, _, err := client.ReadMessage()
	if err == nil {
		t.Fatal("idle websocket unexpectedly received a message")
	}
	if timeout, ok := err.(interface{ Timeout() bool }); ok && timeout.Timeout() {
		t.Fatalf("server did not close idle websocket before client deadline: %v", err)
	}
}

func TestWebsocketHandshakeAndHeartbeatRemainUsable(t *testing.T) {
	node, client := openLifecycleWebsocket(t)
	handshake, err := codec.Encode(packet.Handshake, []byte("{}"))
	if err != nil {
		t.Fatal(err)
	}
	if err := client.WriteMessage(websocket.BinaryMessage, handshake); err != nil {
		t.Fatal(err)
	}
	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	_, response, err := client.ReadMessage()
	if err != nil {
		t.Fatalf("handshake response: %v", err)
	}
	packets, err := codec.NewDecoder().Decode(response)
	if err != nil || len(packets) != 1 || packets[0].Type != packet.Handshake {
		t.Fatalf("handshake packets = %v, error = %v", packets, err)
	}
	ack, _ := codec.Encode(packet.HandshakeAck, nil)
	heartbeat, _ := codec.Encode(packet.Heartbeat, nil)
	if err := client.WriteMessage(websocket.BinaryMessage, append(ack, heartbeat...)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, func() bool {
		node.mu.RLock()
		defer node.mu.RUnlock()
		for _, s := range node.sessions {
			if a, ok := s.NetworkEntity().(*agent); ok && a.status() == statusWorking {
				return true
			}
		}
		return false
	})
	// Established connections must survive the initial handshake deadline.
	time.Sleep(350 * time.Millisecond)
	node.mu.RLock()
	count := len(node.sessions)
	node.mu.RUnlock()
	if count != 1 {
		t.Fatalf("established sessions = %d, want 1", count)
	}
	if err := client.WriteMessage(websocket.BinaryMessage, heartbeat); err != nil {
		t.Fatal(err)
	}
}
