package logic

import (
	"log"

	"github.com/lonng/nano/component"
	"github.com/lonng/nano/examples/demo/tadpole/logic/protocol"
	"github.com/lonng/nano/session"
)

// Manager component
type Manager struct {
	component.Base
}

// NewManager returns  a new manager instance
func NewManager() *Manager {
	return &Manager{}
}

// Login handler was used to guest login
func (m *Manager) Login(ctx *session.RequestContext, msg *protocol.JoyLoginRequest) error {
	log.Println(msg)
	id := ctx.ID()
	ctx.Bind(id)
	return ctx.Response(protocol.LoginResponse{
		Status: protocol.LoginStatusSucc,
		ID:     id,
	})
}
