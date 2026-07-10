package session

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
)

var (
	ErrNotRequest          = errors.New("message is not a request")
	ErrResponseAlreadySent = errors.New("response already sent")
)

// RequestContext owns the lifetime and capabilities of one inbound request.
// It can be passed directly to APIs that accept context.Context.
type RequestContext struct {
	context.Context
	session   *Session
	mid       uint64
	responded atomic.Bool
}

func NewRequestContext(parent context.Context, s *Session, mid uint64) (*RequestContext, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	return &RequestContext{Context: ctx, session: s, mid: mid}, cancel
}

func (c *RequestContext) Session() *Session {
	if c == nil {
		return nil
	}
	return c.session
}

func (c *RequestContext) Response(v interface{}, errCode ...uint64) error {
	if c == nil || c.Context == nil || c.session == nil {
		return errors.New("invalid request context")
	}
	if c.mid == 0 {
		return ErrNotRequest
	}
	if err := c.Err(); err != nil {
		return err
	}
	if !c.responded.CompareAndSwap(false, true) {
		return ErrResponseAlreadySent
	}
	return c.session.ResponseMID(c.mid, v, errCode...)
}

// ResponseTimeout sends the framework timeout response if the request has not
// already produced a response. It is intended for the gateway deadline hook.
func (c *RequestContext) ResponseTimeout(errCode uint64) bool {
	if c == nil || c.session == nil || c.mid == 0 || !c.responded.CompareAndSwap(false, true) {
		return false
	}
	_ = c.session.ResponseMID(c.mid, nil, errCode)
	return true
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
	return c.session.Bind(uid)
}

func (c *RequestContext) ID() int64 {
	if c == nil || c.session == nil {
		return 0
	}
	return c.session.ID()
}

func (c *RequestContext) UID() int64 {
	if c == nil || c.session == nil {
		return 0
	}
	return c.session.UID()
}

func (c *RequestContext) RemoteAddr() net.Addr {
	if c == nil || c.session == nil {
		return nil
	}
	return c.session.RemoteAddr()
}
