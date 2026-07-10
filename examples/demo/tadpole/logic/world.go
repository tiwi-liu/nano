package logic

import (
	"fmt"
	"log"

	"github.com/google/uuid"
	"github.com/lonng/nano"
	"github.com/lonng/nano/component"
	"github.com/lonng/nano/examples/demo/tadpole/logic/protocol"
	"github.com/lonng/nano/session"
)

// World contains all tadpoles
type World struct {
	component.Base
	*nano.Group
}

// NewWorld returns a world instance
func NewWorld() *World {
	return &World{
		Group: nano.NewGroup(uuid.New().String()),
	}
}

// Init initialize world component
func (w *World) Init() {
	session.Lifetime.OnClosed(func(s *session.Session) {
		w.Leave(s)
		w.Broadcast("leave", &protocol.LeaveWorldResponse{ID: s.ID()})
		log.Println(fmt.Sprintf("session count: %d", w.Count()))
	})
}

// Enter was called when new guest enter
func (w *World) Enter(ctx *session.RequestContext, msg []byte) error {
	w.Add(ctx.Session())
	log.Println(fmt.Sprintf("session count: %d", w.Count()))
	return ctx.Response(&protocol.EnterWorldResponse{ID: ctx.ID()})
}

// Update refresh tadpole's position
func (w *World) Update(ctx *session.RequestContext, msg []byte) error {
	return w.Broadcast("update", msg)
}

// Message handler was used to communicate with each other
func (w *World) Message(ctx *session.RequestContext, msg *protocol.WorldMessage) error {
	msg.ID = ctx.ID()
	return w.Broadcast("message", msg)
}
