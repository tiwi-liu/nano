package cluster

import (
	"context"
	"crypto/subtle"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const clusterAuthMetadataKey = "nano-cluster-auth"

func (n *Node) rpcTimeout() time.Duration {
	if n != nil && n.RPCTimeout > 0 {
		return n.RPCTimeout
	}
	return DefaultRPCTimeout
}

func (n *Node) writeTimeout() time.Duration {
	if n != nil && n.WriteTimeout > 0 {
		return n.WriteTimeout
	}
	return DefaultWriteTimeout
}

func (n *Node) rpcContext(parent context.Context, pairs ...string) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, n.rpcTimeout())
	if n != nil && n.ClusterAuthToken != "" {
		pairs = append(pairs, clusterAuthMetadataKey, n.ClusterAuthToken)
	}
	if len(pairs) > 0 {
		ctx = metadata.AppendToOutgoingContext(ctx, pairs...)
	}
	return ctx, cancel
}

func (n *Node) authUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if n == nil || n.ClusterAuthToken == "" {
			return handler(ctx, req)
		}
		values := metadata.ValueFromIncomingContext(ctx, clusterAuthMetadataKey)
		if len(values) != 1 || subtle.ConstantTimeCompare([]byte(values[0]), []byte(n.ClusterAuthToken)) != 1 {
			return nil, status.Errorf(codes.Unauthenticated, "cluster auth failed for %s", info.FullMethod)
		}
		return handler(ctx, req)
	}
}
