package nano

import (
	"net/http"
	"time"

	"github.com/lonng/nano/cluster"
	"github.com/lonng/nano/component"
	"github.com/lonng/nano/internal/env"
	"github.com/lonng/nano/internal/log"
	"github.com/lonng/nano/internal/message"
	"github.com/lonng/nano/pipeline"
	"github.com/lonng/nano/scheduler"
	"github.com/lonng/nano/serialize"
	"github.com/lonng/nano/service"
	"google.golang.org/grpc"
)

type Option func(*cluster.Options)

func WithPipeline(pipeline pipeline.Pipeline) Option {
	return func(opt *cluster.Options) {
		opt.Pipeline = pipeline
	}
}

// WithCustomerRemoteServiceRoute register remote service route
func WithCustomerRemoteServiceRoute(route cluster.CustomerRemoteServiceRoute) Option {
	return func(opt *cluster.Options) {
		opt.RemoteServiceRoute = route
	}
}

// WithAdvertiseAddr sets the advertise address option, it will be the listen address in
// master node and an advertise address which cluster member to connect
func WithAdvertiseAddr(addr string, retryInterval ...time.Duration) Option {
	return func(opt *cluster.Options) {
		opt.AdvertiseAddr = addr
		if len(retryInterval) > 0 {
			opt.RetryInterval = retryInterval[0]
		}
	}
}

// WithMemberAddr sets the listen address which is used to establish connection between
// cluster members. Will select an available port automatically if no member address
// setting and panic if no available port
func WithClientAddr(addr string) Option {
	return func(opt *cluster.Options) {
		opt.ClientAddr = addr
	}
}

// WithMaster sets the option to indicate whether the current node is master node
func WithMaster() Option {
	return func(opt *cluster.Options) {
		opt.IsMaster = true
	}
}

// WithGrpcOptions sets the grpc dial options
func WithGrpcOptions(opts ...grpc.DialOption) Option {
	return func(_ *cluster.Options) {
		env.GrpcOptions = append(env.GrpcOptions, opts...)
	}
}

// WithClusterAuthToken requires the same token on all cluster gRPC calls.
// Empty token disables cluster RPC authentication.
func WithClusterAuthToken(token string) Option {
	return func(opt *cluster.Options) {
		opt.ClusterAuthToken = token
	}
}

// WithComponents sets the Components
func WithComponents(components *component.Components) Option {
	return func(opt *cluster.Options) {
		opt.Components = components
	}
}

// WithHeartbeatInterval sets Heartbeat time interval
func WithHeartbeatInterval(d time.Duration) Option {
	return func(_ *cluster.Options) {
		env.Heartbeat = d
	}
}

// WithCheckOriginFunc sets the function that check `Origin` in http headers
func WithCheckOriginFunc(fn func(*http.Request) bool) Option {
	return func(opt *cluster.Options) {
		env.CheckOrigin = fn
	}
}

// WithDebugMode let 'nano' to run under Debug mode.
func WithDebugMode() Option {
	return func(_ *cluster.Options) {
		env.Debug = true
	}
}

// SetDictionary sets routes map
func WithDictionary(dict map[string]uint16) Option {
	return func(_ *cluster.Options) {
		message.SetDictionary(dict)
	}
}

func WithWSPath(path string) Option {
	return func(_ *cluster.Options) {
		env.WSPath = path
	}
}

// SetTimerPrecision sets the ticker precision, and time precision can not less
// than a Millisecond, and can not change after application running. The default
// precision is time.Second
func WithTimerPrecision(precision time.Duration) Option {
	if precision < time.Millisecond {
		panic("time precision can not less than a Millisecond")
	}
	return func(_ *cluster.Options) {
		env.TimerPrecision = precision
	}
}

// WithSerializer customizes application serializer, which automatically Marshal
// and UnMarshal handler payload
func WithSerializer(serializer serialize.Serializer) Option {
	return func(opt *cluster.Options) {
		env.Serializer = serializer
	}
}

// WithLabel sets the current node label in cluster
func WithLabel(label string) Option {
	return func(opt *cluster.Options) {
		opt.Label = label
	}
}

// WithIsWebsocket indicates whether current node WebSocket is enabled
func WithIsWebsocket(enableWs bool) Option {
	return func(opt *cluster.Options) {
		opt.IsWebsocket = enableWs
	}
}

// WithTSLConfig sets the `key` and `certificate` of TSL
func WithTSLConfig(certificate, key string) Option {
	return func(opt *cluster.Options) {
		opt.TSLCertificate = certificate
		opt.TSLKey = key
	}
}

// WithLogger overrides the default logger
func WithLogger(l log.Logger) Option {
	return func(opt *cluster.Options) {
		log.SetLogger(l)
	}
}

// WithHandshakeValidator sets the function that Verify `handshake` data
func WithHandshakeValidator(fn func([]byte) error) Option {
	return func(opt *cluster.Options) {
		env.HandshakeValidator = fn
	}
}

// WithNodeId set nodeId use snowflake nodeId generate sessionId, default: pid
func WithNodeId(nodeId uint64) Option {
	return func(opt *cluster.Options) {
		service.ResetNodeId(nodeId)
	}
}

// WithScheduler configures the global scheduler concurrency and backlog.
// - workers: number of worker goroutines processing tasks (<=0 means auto)
// - backlog: buffered queue length for pending tasks (<=0 means default)
func WithScheduler(workers, backlog int) Option {
	return func(_ *cluster.Options) {
		scheduler.Configure(workers, backlog)
	}
}

// WithRequestTimeout sets the maximum lifetime of an inbound handler request.
// The timeout includes time spent waiting in the scheduler queue.
func WithRequestTimeout(timeout time.Duration) Option {
	if timeout <= 0 {
		panic("request timeout must be greater than zero")
	}
	return func(opt *cluster.Options) {
		opt.RequestTimeout = timeout
	}
}

// WithRPCTimeout sets the timeout for framework cluster gRPC calls.
func WithRPCTimeout(timeout time.Duration) Option {
	if timeout <= 0 {
		panic("rpc timeout must be greater than zero")
	}
	return func(opt *cluster.Options) {
		opt.RPCTimeout = timeout
	}
}

// WithWriteTimeout sets the socket write deadline for responses, pushes and heartbeats.
func WithWriteTimeout(timeout time.Duration) Option {
	if timeout <= 0 {
		panic("write timeout must be greater than zero")
	}
	return func(opt *cluster.Options) {
		opt.WriteTimeout = timeout
	}
}

// WithUnregisterCallback master unregister member event call fn
func WithUnregisterCallback(fn func(member cluster.Member)) Option {
	return func(opt *cluster.Options) {
		opt.UnregisterCallback = fn
	}
}
