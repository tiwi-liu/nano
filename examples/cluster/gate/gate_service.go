package gate

import (
	"github.com/lonng/nano/component"
	"github.com/lonng/nano/examples/cluster/protocol"
	"github.com/lonng/nano/session"
	"github.com/pingcap/errors"
)

type BindService struct {
	component.Base
	nextGateUid int64
}

func newBindService() *BindService {
	return &BindService{}
}

type (
	LoginRequest struct {
		Nickname string `json:"nickname"`
	}
	LoginResponse struct {
		Code int `json:"code"`
	}
)

func (bs *BindService) Login(ctx *session.RequestContext, msg *LoginRequest) error {
	bs.nextGateUid++
	uid := bs.nextGateUid
	request := &protocol.NewUserRequest{
		Nickname: msg.Nickname,
		GateUid:  uid,
	}
	if err := ctx.RPC("TopicService.NewUser", request); err != nil {
		return errors.Trace(err)
	}
	return ctx.Response(&LoginResponse{})
}

func (bs *BindService) BindChatServer(ctx *session.RequestContext, msg []byte) error {
	return errors.Errorf("not implement")
}
