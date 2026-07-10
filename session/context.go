package session

import (
	"context"
	"errors"
	"net"
	"sync/atomic"

	"github.com/lonng/nano/pkg/errcode"
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

// Response sends a business response. Its system header code is always CodeOk;
// business failures must be represented by the response body.
func (c *RequestContext) Response(v interface{}) error {
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
	return c.session.ResponseMID(c.mid, v, errcode.CodeOk)
}

// RespondSystemError sends a header-only system error if no response has been
// sent. Framework dispatch code owns this operation; business handlers should
// describe their errors in the response body instead.
func (c *RequestContext) RespondSystemError(code errcode.Code) bool {
	if c == nil || c.session == nil || c.mid == 0 || !c.responded.CompareAndSwap(false, true) {
		return false
	}
	_ = c.session.ResponseMID(c.mid, nil, code)
	return true
}

func (c *RequestContext) Responded() bool {
	return c != nil && c.responded.Load()
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
