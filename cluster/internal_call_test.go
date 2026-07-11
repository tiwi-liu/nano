package cluster

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/lonng/nano/cluster/clusterpb"
	"github.com/lonng/nano/component"
	"github.com/lonng/nano/pkg/errcode"
	"github.com/lonng/nano/session"
)

type internalCallService struct{}

func (*internalCallService) Echo(ctx *session.RequestContext, request []byte) error {
	response := append([]byte{}, request...)
	response = append(response, byte(ctx.UID()))
	return ctx.Response(response)
}

func newInternalCallTestNode(t *testing.T) *Node {
	t.Helper()
	receiver := &internalCallService{}
	method, ok := reflect.TypeOf(receiver).MethodByName("Echo")
	if !ok {
		t.Fatal("Echo method not found")
	}
	n := &Node{}
	n.handler = &LocalHandler{
		currentNode: n,
		localHandlers: map[string]*component.Handler{
			"InternalCallService.Echo": {
				Receiver: reflect.ValueOf(receiver),
				Method:   method,
				Type:     reflect.TypeOf([]byte(nil)),
				IsRawArg: true,
			},
		},
	}
	return n
}

func TestNodeCallInvokesRegisteredHandlerAndReturnsBody(t *testing.T) {
	n := newInternalCallTestNode(t)
	response, err := n.Call(context.Background(), &clusterpb.InternalCallRequest{
		Route: "InternalCallService.Echo",
		Data:  []byte("hello"),
		Uid:   7,
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.ErrCode != uint64(errcode.CodeOk) || string(response.Data) != "hello\a" {
		t.Fatalf("response = (%d, %q)", response.ErrCode, response.Data)
	}
}

func TestNodeCallReturnsSystemCodeForMissingMethod(t *testing.T) {
	n := newInternalCallTestNode(t)
	response, err := n.Call(context.Background(), &clusterpb.InternalCallRequest{Route: "Missing.Call"})
	if err != nil {
		t.Fatal(err)
	}
	if response.ErrCode != uint64(errcode.CodeMethodNotFound) {
		t.Fatalf("system code = %d, want CodeMethodNotFound", response.ErrCode)
	}
}

func TestLocalHandlerCallReusesLocalRegisteredHandler(t *testing.T) {
	n := newInternalCallTestNode(t)
	var response []byte
	if err := n.handler.Call(context.Background(), 7, "InternalCallService.Echo", []byte("hello"), &response); err != nil {
		t.Fatal(err)
	}
	if string(response) != "hello\a" {
		t.Fatalf("response = %q", response)
	}
}

func TestLocalHandlerCallReturnsTypedSystemError(t *testing.T) {
	n := newInternalCallTestNode(t)
	err := n.handler.Call(context.Background(), 7, "MissingService.Call", []byte("hello"), &[]byte{})
	var callErr *InternalCallError
	if !errors.As(err, &callErr) || callErr.Code != errcode.CodeServiceNotFound {
		t.Fatalf("error = %v, want CodeServiceNotFound", err)
	}
}
