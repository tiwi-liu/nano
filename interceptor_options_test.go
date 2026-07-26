package nano

import (
	"context"
	"github.com/lonng/nano/cluster"
	"google.golang.org/grpc"
	"testing"
)

func TestUnaryInterceptorsAreStoredOnNodeOptions(t *testing.T) {
	server := func(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		return handler(ctx, req)
	}
	client := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoker(ctx, method, req, reply, cc, opts...)
	}
	options := &cluster.Options{}
	WithUnaryServerInterceptors(server)(options)
	WithUnaryClientInterceptors(client)(options)
	if len(options.UnaryServerInterceptors) != 1 || len(options.UnaryClientInterceptors) != 1 {
		t.Fatalf("interceptors not stored: %+v", options)
	}
}
