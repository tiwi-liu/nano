# Nano 服务治理与可观测性接入

## 设计边界

服务治理能力分成四层：

| 层级 | 所有者 | 内容 |
|---|---|---|
| 框架契约 | `github.com/lonng/nano` | 注册中心接口、成员状态、租约生命周期、路由视图、gRPC interceptor 扩展点 |
| 可选适配器 | `github.com/lonng/nano-contrib` | etcd、OpenTelemetry、Prometheus 的具体实现 |
| 业务服务 | gameServer 等应用 | 服务身份、业务 span、业务指标、业务 Dashboard |
| 共享平台 | `deploy/platform` | etcd、OTel Collector、Jaeger、Prometheus、Grafana |

nano 核心不 import etcd client、OpenTelemetry SDK 或 Prometheus client。不需要服务治理的单机程序因此不会被迫加载相关依赖。

## 注册中心 API

`registry.Registry` 是后端无关的公共契约：

```go
type Registry interface {
    Register(context.Context, Member, RegisterOptions) (Lease, error)
    UpdateStatus(context.Context, string, MemberStatus) error
    Unregister(context.Context, string) error
    List(context.Context) ([]Member, error)
    Watch(context.Context, int64) (<-chan Event, <-chan error)
}
```

成员状态语义：

- `active`：可以接收新流量并参与路由。
- `draining`：停止接收新流量，等待存量请求和连接排空。
- `retired`：已退出服务，不得参与路由。

`registry.Runtime` 负责注册、watch 本节点状态、关闭租约和计算 `AllowNewTraffic()`。它通过 `registry.Observer` 上报状态变化，不依赖任何指标实现。

## etcd 接入

etcd 实现位于 `nano-contrib/registry/etcd`：

```go
client, err := clientv3.New(clientv3.Config{
    Endpoints:   []string{"127.0.0.1:2379"},
    DialTimeout: 3 * time.Second,
})
if err != nil {
    return err
}

runtime, err := registry.StartRuntime(
    ctx,
    etcdregistry.New(client, "/nano/prod"),
    registry.RuntimeOptions{
        Role:     "game",
        TTL:      15 * time.Second,
        Observer: metrics.RegistryObserver{},
        Member: registry.Member{
            ID:          "game-1",
            ServiceAddr: "10.0.0.10:34580",
            Services:    []string{"GameService"},
        },
    },
)
```

服务关闭时必须调用 `runtime.Close(ctx)`，释放 lease。生产环境应在停止 Pod 前先切换为 `draining`，并让 readiness 返回失败，完成摘流后再终止进程。

## 跨节点 Trace

nano 提供以下公共 Option：

```go
nano.WithUnaryClientInterceptors(...grpc.UnaryClientInterceptor)
nano.WithUnaryServerInterceptors(...grpc.UnaryServerInterceptor)
```

OpenTelemetry 适配器通过它们注入和提取 W3C `traceparent`：

```go
nano.Listen(addr,
    nano.WithUnaryClientInterceptors(oteladapter.UnaryClientInterceptor()),
    nano.WithUnaryServerInterceptors(oteladapter.UnaryServerInterceptor()),
)
```

server interceptor 的执行顺序是：框架集群认证 interceptor 在最外层，然后执行应用传入的 interceptor。认证失败的请求不会进入业务 Trace。

框架负责跨节点 RPC span 和上下文传播；业务服务继续负责 `EnterSNG`、支付、开桌等业务 span。

## Prometheus 与健康检查

`nano-contrib/observability/prometheus` 提供：

- `/metrics`
- `/healthz`
- `/readyz`
- nano Handler 请求量和耗时
- registry 成员状态
- Go runtime 和进程指标

业务指标由应用创建，然后注册到共享 Registry：

```go
var TablesStarted = prometheus.NewCounterVec(...)

func init() {
    frameworkmetrics.MustRegister(TablesStarted)
}
```

不要在 nano 核心增加游戏房间、牌桌、支付等领域指标。

## 当前兼容边界

etcd 目前尚未替代 nano master：

```text
etcd          -> 注册、lease、active/draining/retired、readiness
nano master   -> cluster member 分发、remote service view、实际路由选择
```

因此生产集群当前仍必须启动 master。后续只有在 `registry.View` 正式接管 nano 的成员表、远端服务视图和路由选择，并完成混合版本测试后，才能移除 master。

## 新服务最小接入清单

新服务不再复制 gameServer 的 `internal/registry` 或 `internal/observability/tracing`，只需要：

1. 依赖 nano 和需要的 nano-contrib 子包。
2. 配置服务名、版本、环境和节点 ID。
3. 初始化 OTel，并给 nano 注入 client/server interceptor。
4. 创建 etcd client 和 `registry.Runtime`。
5. 将 `runtime.AllowNewTraffic()` 关联到 readiness 或入口握手检查。
6. 注册自己的业务指标和业务 span。
7. 复用工作区 `deploy/platform`，不在服务工程重复部署 Grafana、Collector、Jaeger、Prometheus 和 etcd。

## 发版与升级顺序

新 `registry` 包尚未进入已发布的 nano v0.5.1，因此当前工作区使用本地 `replace`。正式发版顺序必须是：

1. 发布包含 `registry` 和 interceptor Option 的新版 nano。
2. nano-contrib 更新到该 nano 版本，删除本地 replace 并发布首个版本。
3. gameServer 等业务工程更新版本，删除对本地 module 的 replace。

不要先发布 nano-contrib，否则外部构建会解析到不包含 `registry` 包的 nano v0.5.1。

