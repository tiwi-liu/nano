package cluster

import (
	"context"
	"errors"
	"net"
	"reflect"
	"testing"

	"github.com/lonng/nano/cluster/clusterpb"
	"github.com/lonng/nano/component"
	"github.com/lonng/nano/internal/message"
	"github.com/lonng/nano/pkg/errcode"
	"github.com/lonng/nano/session"
	"google.golang.org/grpc/metadata"
)

type handlerResponseEntity struct {
	code   errcode.Code
	body   interface{}
	err    error
	pushes int
}

func (e *handlerResponseEntity) Push(string, interface{}) error {
	e.pushes++
	return nil
}
func (e *handlerResponseEntity) RPC(string, interface{}) error { return nil }
func (e *handlerResponseEntity) SendResponse(_ uint64, code errcode.Code, body interface{}) error {
	if e.err != nil {
		err := e.err
		e.err = nil
		return err
	}
	e.code = code
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
func (*responseContractService) UIDConflict(ctx *session.RequestContext, _ *[]byte) error {
	if err := ctx.Bind(100); err != nil {
		return err
	}
	return ctx.Bind(200)
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

func TestFailedBusinessResponseFallsBackToSystemError(t *testing.T) {
	entity := &handlerResponseEntity{err: errors.New("gateway unavailable")}
	ctx, cancel := session.NewRequestContext(context.Background(), session.New(entity), 1)
	defer cancel()
	receiver := &responseContractService{}
	method, ok := reflect.TypeOf(receiver).MethodByName("Responds")
	if !ok {
		t.Fatal("Responds method not found")
	}
	handler := &component.Handler{Receiver: reflect.ValueOf(receiver), Method: method}
	args := []reflect.Value{handler.Receiver, reflect.ValueOf(ctx), reflect.ValueOf(&[]byte{})}

	invokeHandler(handler, args, ctx, &message.Message{Type: message.Request, Route: "Service.Responds"})
	if entity.code != errcode.CodeInternalErr {
		t.Fatalf("system code = %d, want CodeInternalErr", entity.code)
	}
	if entity.body != nil {
		t.Fatalf("system error body = %v, want nil", entity.body)
	}
}

func TestHandlerPanicGetsInternalSystemError(t *testing.T) {
	entity := invokeResponseContractHandler(t, "Panics")
	if entity.code != errcode.CodeInternalErr {
		t.Fatalf("system code = %d, want CodeInternalErr", entity.code)
	}
}

func TestForwardedUIDMismatchGetsPermissionDenied(t *testing.T) {
	entity := &handlerResponseEntity{}
	s := session.New(entity)
	if err := s.Bind(100); err != nil {
		t.Fatal(err)
	}

	if bindForwardedUID(s, 200, 9) {
		t.Fatal("mismatched forwarded UID should be rejected")
	}
	if entity.code != errcode.CodePermissionDenied {
		t.Fatalf("system code = %d, want CodePermissionDenied", entity.code)
	}
	if got := s.UID(); got != 100 {
		t.Fatalf("session UID = %d, want original UID 100", got)
	}
}

func TestForwardedUIDRequiresSingleValidMetadataValue(t *testing.T) {
	validContext := metadata.NewIncomingContext(context.Background(), metadata.Pairs("uid", "100"))
	if uid, err := forwardedUID(validContext); err != nil || uid != 100 {
		t.Fatalf("forwardedUID(valid) = (%d, %v)", uid, err)
	}

	invalidContexts := []context.Context{
		context.Background(),
		metadata.NewIncomingContext(context.Background(), metadata.Pairs("uid", "invalid")),
		metadata.NewIncomingContext(context.Background(), metadata.Pairs("uid", "-1")),
		metadata.NewIncomingContext(context.Background(), metadata.Pairs("uid", "100", "uid", "200")),
	}
	for _, ctx := range invalidContexts {
		if _, err := forwardedUID(ctx); err == nil {
			t.Fatal("invalid forwarded UID metadata was accepted")
		}
	}
}

func TestHandlerUIDConflictGetsPermissionDenied(t *testing.T) {
	entity := invokeResponseContractHandler(t, "UIDConflict")
	if entity.code != errcode.CodePermissionDenied {
		t.Fatalf("system code = %d, want CodePermissionDenied", entity.code)
	}
}

func TestHandleResponsePropagatesUIDToGatewaySession(t *testing.T) {
	entity := &handlerResponseEntity{}
	s := session.New(entity)
	n := &Node{sessions: map[int64]*session.Session{10: s}}

	_, err := n.HandleResponse(context.Background(), &clusterpb.ResponseMessage{
		SessionId: 10,
		Id:        7,
		Uid:       100,
		Data:      []byte("ok"),
		ErrCode:   uint64(errcode.CodeOk),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := s.UID(); got != 100 {
		t.Fatalf("gateway session UID = %d, want propagated UID 100", got)
	}
}

func TestHandleResponseNotifiesFirstGatewayUIDBindingOnce(t *testing.T) {
	entity := &handlerResponseEntity{}
	s := session.New(entity)
	bound := 0
	n := &Node{
		Options: Options{SessionBoundCallback: func(got *session.Session) {
			if got != s {
				t.Fatalf("bound session = %p, want %p", got, s)
			}
			bound++
		}},
		sessions: map[int64]*session.Session{10: s},
	}

	for id := uint64(1); id <= 2; id++ {
		if _, err := n.HandleResponse(context.Background(), &clusterpb.ResponseMessage{
			SessionId: 10, Id: id, Uid: 100, ErrCode: uint64(errcode.CodeOk),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if bound != 1 {
		t.Fatalf("binding callbacks = %d, want 1", bound)
	}
}

func TestHandlePushRejectsStaleConnectionEpoch(t *testing.T) {
	entity := &handlerResponseEntity{}
	client := session.New(entity)
	if err := client.Bind(100); err != nil {
		t.Fatal(err)
	}
	client.SetConnectionEpoch(2)
	n := &Node{sessions: map[int64]*session.Session{client.ID(): client}}

	if _, err := n.HandlePush(context.Background(), &clusterpb.PushMessage{
		SessionId: client.ID(), Route: "UserService.Notify", Uid: 100, ConnectionEpoch: 1,
	}); err == nil {
		t.Fatal("stale connection epoch should be rejected")
	}
	if entity.pushes != 0 {
		t.Fatalf("pushes = %d, want 0", entity.pushes)
	}
	if _, err := n.HandlePush(context.Background(), &clusterpb.PushMessage{
		SessionId: client.ID(), Route: "UserService.Notify", Uid: 100, ConnectionEpoch: 2,
	}); err != nil {
		t.Fatal(err)
	}
	if entity.pushes != 1 {
		t.Fatalf("pushes = %d, want 1", entity.pushes)
	}
}

func TestGatewayRouteUsesAdvertisedMemberAddress(t *testing.T) {
	entity := &handlerResponseEntity{}
	s := session.New(entity)
	node := &Node{
		ServiceAddr: "0.0.0.0:34591",
		Options:     Options{MemberAddr: "nano-gate:34591"},
	}
	handler := &LocalHandler{currentNode: node}

	gateAddr, sessionID := handler.gatewayRoute(s)
	if gateAddr != "nano-gate:34591" {
		t.Fatalf("gateAddr = %q, want advertised member address", gateAddr)
	}
	if sessionID != s.ID() {
		t.Fatalf("sessionID = %d, want %d", sessionID, s.ID())
	}
}

func TestGatewayRouteKeepsOriginalGateAddressForForwardedSession(t *testing.T) {
	entity := &acceptor{gateAddr: "nano-gate-a:34591", sid: 99}
	s := session.New(entity)
	node := &Node{
		ServiceAddr: "0.0.0.0:34580",
		Options:     Options{MemberAddr: "nano-node-game:34580"},
	}
	handler := &LocalHandler{currentNode: node}

	gateAddr, sessionID := handler.gatewayRoute(s)
	if gateAddr != "nano-gate-a:34591" || sessionID != 99 {
		t.Fatalf("gatewayRoute = (%q, %d), want forwarded gate route", gateAddr, sessionID)
	}
}
