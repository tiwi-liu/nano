package session

import (
	"context"
	"errors"
	"net"
	"sync/atomic"

	"github.com/lonng/nano/pkg/errcode"
)

var (
	ErrNotRequest              = errors.New("message is not a request")
	ErrResponseAlreadySent     = errors.New("response already sent")
	ErrInternalCallUnavailable = errors.New("internal call is unavailable")
)

type InternalCaller interface {
	Call(ctx context.Context, uid int64, route string, request, response interface{}) error
}

type ResponseSender func(value interface{}, code errcode.Code) error

const (
	responseOpen int32 = iota
	responseSending
	responseSent
)

// RequestContext owns the lifetime and capabilities of one inbound request.
// It can be passed directly to APIs that accept context.Context.
type RequestContext struct {
	context.Context
	session       *Session
	mid           uint64
	uid           atomic.Int64
	responseState atomic.Int32
	caller        InternalCaller
	sender        ResponseSender
}

func NewRequestContext(parent context.Context, s *Session, mid uint64, callers ...InternalCaller) (*RequestContext, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	requestContext := &RequestContext{Context: ctx, session: s, mid: mid}
	if len(callers) > 0 {
		requestContext.caller = callers[0]
	}
	if s != nil {
		requestContext.uid.Store(s.UID())
		if mid > 0 {
			requestContext.sender = func(value interface{}, code errcode.Code) error {
				return s.NetworkEntity().SendResponse(mid, code, value)
			}
		}
	}
	return requestContext, cancel
}

func NewInternalRequestContext(parent context.Context, uid int64, caller InternalCaller, sender ResponseSender) (*RequestContext, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	requestContext := &RequestContext{Context: ctx, caller: caller, sender: sender}
	requestContext.uid.Store(uid)
	return requestContext, cancel
}

func (c *RequestContext) Call(route string, request, response interface{}) error {
	if c == nil || c.Context == nil || c.caller == nil {
		return ErrInternalCallUnavailable
	}
	if err := c.Err(); err != nil {
		return err
	}
	return c.caller.Call(c.Context, c.UID(), route, request, response)
}

func (c *RequestContext) Session() *Session {
	if c == nil {
		return nil
	}
	return c.session
}

// Response sends a business response. Its system header code is always CodeOk;
// business failures must be represented by the response body.
func (c *RequestContext) Response(v interface{}) error {
	if c == nil || c.Context == nil {
		return errors.New("invalid request context")
	}
	if c.sender == nil {
		if c.mid == 0 {
			return ErrNotRequest
		}
		return errors.New("invalid request context")
	}
	if err := c.Err(); err != nil {
		return err
	}
	if !c.responseState.CompareAndSwap(responseOpen, responseSending) {
		return ErrResponseAlreadySent
	}
	if err := c.sender(v, errcode.CodeOk); err != nil {
		c.responseState.Store(responseOpen)
		return err
	}
	c.responseState.Store(responseSent)
	return nil
}

// RespondSystemError sends a header-only system error if no response has been
// sent. Framework dispatch code owns this operation; business handlers should
// describe their errors in the response body instead.
func (c *RequestContext) RespondSystemError(code errcode.Code) bool {
	if c == nil || c.sender == nil || !c.responseState.CompareAndSwap(responseOpen, responseSending) {
		return false
	}
	if err := c.sender(nil, code); err != nil {
		c.responseState.Store(responseOpen)
		return false
	}
	c.responseState.Store(responseSent)
	return true
}

func (c *RequestContext) Responded() bool {
	return c != nil && c.responseState.Load() == responseSent
}

func (c *RequestContext) Push(route string, v interface{}) error {
	if c == nil || c.session == nil {
		return errors.New("invalid request context")
	}
	return c.session.Push(route, v)
}

func (c *RequestContext) RPC(route string, v interface{}) error {
	if c == nil || c.session == nil {
		return errors.New("invalid request context")
	}
	return c.session.RPC(route, v)
}

func (c *RequestContext) Bind(uid int64) error {
	if c == nil || c.session == nil {
		return errors.New("invalid request context")
	}
	if err := c.session.Bind(uid); err != nil {
		return err
	}
	c.uid.Store(uid)
	return nil
}

func (c *RequestContext) ID() int64 {
	if c == nil || c.session == nil {
		return 0
	}
	return c.session.ID()
}

func (c *RequestContext) UID() int64 {
	if c == nil {
		return 0
	}
	return c.uid.Load()
}

func (c *RequestContext) RemoteAddr() net.Addr {
	if c == nil || c.session == nil {
		return nil
	}
	return c.session.RemoteAddr()
}
