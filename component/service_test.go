package component

import (
	"testing"

	"github.com/lonng/nano/session"
)

type ContextService struct {
	Base
}

type ContextRequest struct {
	Value string
}

func (s *ContextService) Login(ctx *session.RequestContext, req *ContextRequest) error {
	return ctx.Response(req)
}

func TestServiceExtractsContextHandler(t *testing.T) {
	service := NewService(&ContextService{}, nil)
	if err := service.ExtractHandler(); err != nil {
		t.Fatal(err)
	}

	_, ok := service.Handlers["Login"]
	if !ok {
		t.Fatal("Login handler was not registered")
	}
}

type LegacySessionService struct{ Base }

func (s *LegacySessionService) Login(_ *session.Session, _ *ContextRequest) error { return nil }

func TestServiceRejectsLegacySessionHandler(t *testing.T) {
	service := NewService(&LegacySessionService{}, nil)
	if err := service.ExtractHandler(); err == nil {
		t.Fatal("legacy *session.Session handler should be rejected")
	}
}
