package cluster

import (
	"context"
	"errors"
	"io"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/lonng/nano/cluster/clusterpb"
	"github.com/lonng/nano/internal/packet"
	"github.com/lonng/nano/pkg/errcode"
	"github.com/lonng/nano/session"
	"google.golang.org/grpc"
)

type kickTestEntity struct {
	kickErr error
	events  []string
}

func (*kickTestEntity) Push(string, interface{}) error                       { return nil }
func (*kickTestEntity) RPC(string, interface{}) error                        { return nil }
func (*kickTestEntity) SendResponse(uint64, errcode.Code, interface{}) error { return nil }
func (e *kickTestEntity) Close() error {
	e.events = append(e.events, "close")
	return nil
}
func (e *kickTestEntity) Kick([]byte) error {
	e.events = append(e.events, "kick")
	return e.kickErr
}
func (*kickTestEntity) RemoteAddr() net.Addr { return &net.TCPAddr{} }

type kickRecordingMemberClient struct {
	clusterpb.MemberClient
	request *clusterpb.CloseSessionRequest
}

func (c *kickRecordingMemberClient) CloseSession(_ context.Context, request *clusterpb.CloseSessionRequest, _ ...grpc.CallOption) (*clusterpb.CloseSessionResponse, error) {
	c.request = request
	return &clusterpb.CloseSessionResponse{}, nil
}

func TestAgentKickWritesControlPacketBeforeClosingConnection(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()
	agent := newAgent(server, nil, nil, 0)
	kicker, ok := interface{}(agent).(interface{ Kick([]byte) error })
	if !ok {
		t.Fatal("agent does not implement Kick")
	}
	go agent.write()

	if err := kicker.Kick(nil); err != nil {
		t.Fatalf("Kick() error = %v", err)
	}
	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	header := make([]byte, 4)
	if _, err := io.ReadFull(client, header); err != nil {
		t.Fatalf("read kick header: %v", err)
	}
	if header[0] != byte(packet.Kick) || header[1] != 0 || header[2] != 0 || header[3] != 0 {
		t.Fatalf("kick header = %v, want [%d 0 0 0]", header, packet.Kick)
	}
	if _, err := client.Read(make([]byte, 1)); err == nil {
		t.Fatal("connection remained open after kick packet")
	}
}

func TestAcceptorKickForwardsKickRequestToGateway(t *testing.T) {
	gateClient := &kickRecordingMemberClient{}
	client := &acceptor{
		sid:        42,
		gateClient: gateClient,
		node:       &Node{},
	}
	body := []byte("reason")

	if err := client.Kick(body); err != nil {
		t.Fatalf("Kick() error = %v", err)
	}
	if gateClient.request == nil {
		t.Fatal("gateway did not receive CloseSession request")
	}
	if gateClient.request.GetSessionId() != 42 || !gateClient.request.GetKick() || !reflect.DeepEqual(gateClient.request.GetData(), body) {
		t.Fatalf("CloseSession request = %+v", gateClient.request)
	}
}

func TestNodeCloseSessionUsesRequestedDisconnectMode(t *testing.T) {
	tests := []struct {
		name        string
		requestKick bool
		kickErr     error
		wantEvents  []string
	}{
		{name: "kick", requestKick: true, wantEvents: []string{"kick"}},
		{name: "kick failure falls back to close", requestKick: true, kickErr: errors.New("kick failed"), wantEvents: []string{"kick", "close"}},
		{name: "ordinary close", wantEvents: []string{"close"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entity := &kickTestEntity{kickErr: tt.kickErr}
			client := session.New(entity)
			node := &Node{sessions: map[int64]*session.Session{client.ID(): client}}

			if _, err := node.CloseSession(context.Background(), &clusterpb.CloseSessionRequest{
				SessionId: client.ID(),
				Kick:      tt.requestKick,
			}); err != nil {
				t.Fatalf("CloseSession() error = %v", err)
			}
			if !reflect.DeepEqual(entity.events, tt.wantEvents) {
				t.Fatalf("events = %v, want %v", entity.events, tt.wantEvents)
			}
			if _, found := node.sessions[client.ID()]; found {
				t.Fatal("session remained registered after CloseSession")
			}
		})
	}
}

func TestNodeCloseSessionRejectsStaleConnectionEpoch(t *testing.T) {
	entity := &kickTestEntity{}
	client := session.New(entity)
	if err := client.Bind(100); err != nil {
		t.Fatal(err)
	}
	client.SetConnectionEpoch(7)
	node := &Node{sessions: map[int64]*session.Session{client.ID(): client}}

	if _, err := node.CloseSession(context.Background(), &clusterpb.CloseSessionRequest{
		SessionId: client.ID(), Kick: true, Uid: 100, ConnectionEpoch: 6,
	}); err == nil {
		t.Fatal("stale connection epoch should be rejected")
	}
	if len(entity.events) != 0 {
		t.Fatalf("events = %v, want no disconnect", entity.events)
	}
	if _, found := node.sessions[client.ID()]; !found {
		t.Fatal("current session was removed by stale close request")
	}
}
