package cluster

import (
	"context"
	"errors"
	"net"
	"reflect"
	"testing"

	"github.com/lonng/nano/component"
	"github.com/lonng/nano/internal/message"
	"github.com/lonng/nano/pkg/errcode"
	"github.com/lonng/nano/session"
)

type handlerResponseEntity struct {
	code errcode.Code
	body interface{}
}

func (e *handlerResponseEntity) Push(string, interface{}) error { return nil }
func (e *handlerResponseEntity) RPC(string, interface{}) error  { return nil }
func (e *handlerResponseEntity) ResponseMid(_ uint64, code uint64, body interface{}) error {
	e.code = errcode.Code(code)
	e.body = body
	return nil
}
func (e *handlerResponseEntity) Close() error         { return nil }
func (e *handlerResponseEntity) RemoteAddr() net.Addr { return nil }

type responseContractService struct{}

func (*responseContractService) NoResponse(*session.RequestContext, *[]byte) error { return nil }
func (*responseContractService) ReturnsError(*session.RequestContext, *[]byte) error {
	return errors.New("storage unavailable")
}
func (*responseContractService) Responds(ctx *session.RequestContext, _ *[]byte) error {
	return ctx.Response("business response")
}
func (*responseContractService) Panics(*session.RequestContext, *[]byte) error {
	panic("unexpected failure")
}

func invokeResponseContractHandler(t *testing.T, methodName string) *handlerResponseEntity {
	t.Helper()
	entity := &handlerResponseEntity{}
	ctx, cancel := session.NewRequestContext(context.Background(), session.New(entity), 1)
	defer cancel()
	receiver := &responseContractService{}
	method, ok := reflect.TypeOf(receiver).MethodByName(methodName)
	if !ok {
		t.Fatalf("method %s not found", methodName)
	}
	handler := &component.Handler{Receiver: reflect.ValueOf(receiver), Method: method}
	args := []reflect.Value{handler.Receiver, reflect.ValueOf(ctx), reflect.ValueOf(&[]byte{})}

	invokeHandler(handler, args, ctx, &message.Message{Type: message.Request, Route: "Service." + methodName})
	return entity
}

func TestHandlerWithoutResponseGetsInternalSystemError(t *testing.T) {
	entity := invokeResponseContractHandler(t, "NoResponse")
	if entity.code != errcode.CodeInternalErr {
		t.Fatalf("system code = %d, want CodeInternalErr", entity.code)
	}
}

func TestHandlerErrorGetsInternalSystemError(t *testing.T) {
	entity := invokeResponseContractHandler(t, "ReturnsError")
	if entity.code != errcode.CodeInternalErr {
		t.Fatalf("system code = %d, want CodeInternalErr", entity.code)
	}
}

func TestBusinessResponseKeepsSystemCodeOK(t *testing.T) {
	entity := invokeResponseContractHandler(t, "Responds")
	if entity.code != errcode.CodeOk || entity.body != "business response" {
		t.Fatalf("response = (%d, %v), want (CodeOk, business response)", entity.code, entity.body)
	}
}

func TestHandlerPanicGetsInternalSystemError(t *testing.T) {
	entity := invokeResponseContractHandler(t, "Panics")
	if entity.code != errcode.CodeInternalErr {
		t.Fatalf("system code = %d, want CodeInternalErr", entity.code)
	}
}
