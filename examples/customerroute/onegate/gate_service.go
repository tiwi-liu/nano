package onegate

import (
	"fmt"
	"github.com/lonng/nano/component"
	"github.com/lonng/nano/examples/cluster/protocol"
	"github.com/lonng/nano/session"
	"github.com/pingcap/errors"
)

type RegisterService struct {
	component.Base
	nextGateUid int64
}

func newRegisterService() *RegisterService {
	return &RegisterService{}
}

type (
	RegisterRequest struct {
		Nickname string `json:"nickname"`
	}
	RegisterResponse struct {
		Code int `json:"code"`
	}
)

func (bs *RegisterService) Login(ctx *session.RequestContext, msg *RegisterRequest) error {
	bs.nextGateUid++
	uid := bs.nextGateUid
	ctx.Bind(uid)
	fmt.Println("Login uid:", uid)
	chat := &protocol.JoinRoomRequest{
		Nickname:  msg.Nickname,
		GateUid:   uid,
		MasterUid: uid,
	}
	if err := ctx.RPC("ChatRoomService.JoinRoom", chat); err != nil {
		return errors.Trace(err)
	}
	return ctx.Response(&RegisterResponse{})
}
