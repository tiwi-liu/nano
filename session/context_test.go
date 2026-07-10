package session

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lonng/nano/pkg/errcode"
)

type requestContextTestEntity struct {
	responses      map[uint64]interface{}
	responseErrors map[uint64]uint64
	pushes         map[string]interface{}
}

func newResponderTestEntity() *requestContextTestEntity {
	return &requestContextTestEntity{
		responses:      map[uint64]interface{}{},
		responseErrors: map[uint64]uint64{},
		pushes:         map[string]interface{}{},
	}
}

func (e *requestContextTestEntity) Push(route string, v interface{}) error {
	e.pushes[route] = v
	return nil
}

func (e *requestContextTestEntity) RPC(string, interface{}) error { return nil }
func (e *requestContextTestEntity) ResponseMid(mid uint64, errCode uint64, v interface{}) error {
	e.responses[mid] = v
	e.responseErrors[mid] = errCode
	return nil
}
func (e *requestContextTestEntity) Close() error         { return nil }
func (e *requestContextTestEntity) RemoteAddr() net.Addr { return nil }

func TestRequestContextBindsResponseToCapturedMID(t *testing.T) {
	entity := newResponderTestEntity()
	s := New(entity)

	first, cancelFirst := NewRequestContext(context.Background(), s, 1)
	defer cancelFirst()
	second, cancelSecond := NewRequestContext(context.Background(), s, 2)
	defer cancelSecond()

	if err := second.Response("second"); err != nil {
		t.Fatal(err)
	}
	if err := first.Response("first"); err != nil {
		t.Fatal(err)
	}

	if got := entity.responses[1]; got != "first" {
		t.Fatalf("mid 1 response = %v, want first", got)
	}
	if got := entity.responses[2]; got != "second" {
		t.Fatalf("mid 2 response = %v, want second", got)
	}
	if got := entity.responseErrors[1]; got != uint64(errcode.CodeOk) {
		t.Fatalf("business response system code = %d, want CodeOk", got)
	}
}

func TestRequestContextPropagatesDeadline(t *testing.T) {
	parent, parentCancel := context.WithTimeout(context.Background(), time.Hour)
	defer parentCancel()

	ctx, cancel := NewRequestContext(parent, New(newResponderTestEntity()), 1)
	defer cancel()

	want, _ := parent.Deadline()
	got, ok := ctx.Deadline()
	if !ok || !got.Equal(want) {
		t.Fatalf("deadline = %v, %v; want %v, true", got, ok, want)
	}
}

func TestRequestContextCapturesUIDSnapshot(t *testing.T) {
	s := New(newResponderTestEntity())
	if err := s.Bind(100); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := NewRequestContext(context.Background(), s, 1)
	defer cancel()

	// Simulate an unexpected concurrent mutation of the connection identity.
	atomic.StoreInt64(&s.uid, 200)
	if got := ctx.UID(); got != 100 {
		t.Fatalf("request UID = %d, want captured UID 100", got)
	}
}

func TestRequestContextBindUpdatesRequestIdentity(t *testing.T) {
	ctx, cancel := NewRequestContext(context.Background(), New(newResponderTestEntity()), 1)
	defer cancel()

	if err := ctx.Bind(100); err != nil {
		t.Fatal(err)
	}
	if got := ctx.UID(); got != 100 {
		t.Fatalf("request UID = %d, want newly bound UID 100", got)
	}
}

func TestRequestContextRejectsResponseAfterCancellation(t *testing.T) {
	entity := newResponderTestEntity()
	ctx, cancel := NewRequestContext(context.Background(), New(entity), 1)
	cancel()

	if err := ctx.Response("late"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Response error = %v, want context.Canceled", err)
	}
	if len(entity.responses) != 0 {
		t.Fatalf("responses = %v, want none", entity.responses)
	}
}

func TestRequestContextRejectsResponseWithoutRequestMID(t *testing.T) {
	ctx, cancel := NewRequestContext(context.Background(), New(newResponderTestEntity()), 0)
	defer cancel()

	if err := ctx.Response("invalid"); !errors.Is(err, ErrNotRequest) {
		t.Fatalf("Response error = %v, want ErrNotRequest", err)
	}
}

func TestRequestContextSystemErrorResponseIsSentAtMostOnce(t *testing.T) {
	entity := newResponderTestEntity()
	ctx, cancel := NewRequestContext(context.Background(), New(entity), 7)
	defer cancel()

	if !ctx.RespondSystemError(errcode.CodeRequestTimeout) {
		t.Fatal("first timeout response should be sent")
	}
	if ctx.RespondSystemError(errcode.CodeRequestTimeout) {
		t.Fatal("second timeout response should be ignored")
	}
	if err := ctx.Response("late"); !errors.Is(err, ErrResponseAlreadySent) {
		t.Fatalf("late response error = %v, want ErrResponseAlreadySent", err)
	}
	if got := entity.responseErrors[7]; got != uint64(errcode.CodeRequestTimeout) {
		t.Fatalf("timeout error code = %d, want %d", got, errcode.CodeRequestTimeout)
	}
}

func TestRequestContextPushUsesSession(t *testing.T) {
	entity := newResponderTestEntity()
	ctx, cancel := NewRequestContext(context.Background(), New(entity), 1)
	defer cancel()

	if err := ctx.Push("UserService.Notify", "hello"); err != nil {
		t.Fatal(err)
	}
	if got := entity.pushes["UserService.Notify"]; got != "hello" {
		t.Fatalf("push payload = %v, want hello", got)
	}
}
