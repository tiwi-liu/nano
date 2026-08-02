package cluster

import (
	"context"
	"net"

	"github.com/lonng/nano/cluster/clusterpb"
	"github.com/lonng/nano/internal/message"
	"github.com/lonng/nano/mock"
	"github.com/lonng/nano/pkg/errcode"
	"github.com/lonng/nano/session"
)

type acceptor struct {
	sid        int64
	gateClient clusterpb.MemberClient
	session    *session.Session
	rpcHandler rpcHandler
	gateAddr   string
	node       *Node
}

// Push implements the session.NetworkEntity interface
func (a *acceptor) Push(route string, v interface{}) error {
	// TODO: buffer
	data, err := message.Serialize(v)
	if err != nil {
		return err
	}
	request := &clusterpb.PushMessage{
		SessionId: a.sid,
		Route:     route,
		Data:      data,
	}
	ctx, cancel := a.node.rpcContext(context.Background())
	_, err = a.gateClient.HandlePush(ctx, request)
	cancel()
	return err
}

// RPC implements the session.NetworkEntity interface
func (a *acceptor) RPC(route string, v interface{}) error {
	// TODO: buffer
	data, err := message.Serialize(v)
	if err != nil {
		return err
	}
	msg := &message.Message{
		Type:  message.Notify,
		Route: route,
		Data:  data,
	}
	a.rpcHandler(a.session, msg, true)
	return nil
}

// SendResponse implements the session.NetworkEntity interface
func (a *acceptor) SendResponse(mid uint64, code errcode.Code, v interface{}) error {
	var data []byte
	if code == errcode.CodeOk {
		var err error
		data, err = message.Serialize(v)
		if err != nil {
			return err
		}
	}
	request := &clusterpb.ResponseMessage{
		SessionId: a.sid,
		Id:        mid,
		Data:      data,
		ErrCode:   uint64(code),
		Uid:       a.session.UID(),
	}
	ctx, cancel := a.node.rpcContext(context.Background())
	_, err := a.gateClient.HandleResponse(ctx, request)
	cancel()
	return err
}

// Kick asks the gateway that owns the client connection to send a kick packet
// before closing the session.
func (a *acceptor) Kick(data []byte) error {
	request := &clusterpb.CloseSessionRequest{
		SessionId: a.sid,
		Kick:      true,
		Data:      data,
	}
	ctx, cancel := a.node.rpcContext(context.Background())
	_, err := a.gateClient.CloseSession(ctx, request)
	cancel()
	return err
}

// Close implements the session.NetworkEntity interface
func (a *acceptor) Close() error {
	// TODO: buffer
	request := &clusterpb.CloseSessionRequest{
		SessionId: a.sid,
	}
	ctx, cancel := a.node.rpcContext(context.Background())
	_, err := a.gateClient.CloseSession(ctx, request)
	cancel()
	return err
}

// RemoteAddr implements the session.NetworkEntity interface
func (*acceptor) RemoteAddr() net.Addr {
	return mock.NetAddr{}
}
