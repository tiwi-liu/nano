# Nano 框架架构 Review

## 状态

- 日期：2026-07-26
- 范围：`nano/` 框架整体，包括连接层、协议层、Session、Handler 调度、Scheduler、Cluster/gRPC、组件注册、测试与运维能力。
- 目标：归档当前架构、指出主要风险、给出修复优先级和落地建议。

## 当前架构

Nano 当前是一个轻量游戏服务器网络框架，核心链路如下：

```text
client tcp/ws
  -> cluster.agent read loop
  -> codec packet decode
  -> message decode
  -> LocalHandler route dispatch
  -> scheduler task queue
  -> component handler
  -> RequestContext.Response / Push / Call
  -> agent write loop / acceptor gRPC relay
```

模块职责：

| 模块 | 当前职责 |
|---|---|
| `interface.go` / `options.go` | 启动入口、全局 Option、Signal 退出 |
| `cluster` | TCP/WS 网关、节点注册、跨节点转发、内部 RPC |
| `session` | 连接上下文、UID 绑定、请求上下文、Session 临时数据 |
| `component` | 通过反射注册 Service 和 Handler |
| `scheduler` | 全局 Handler worker 池、Timer |
| `internal/codec` | Pomelo 风格 packet 编解码 |
| `internal/message` | request/notify/response/push 消息编解码 |
| `pipeline` | 入站/出站消息扩展点 |
| `serialize` | JSON / protobuf 序列化 |
| `registry` | 注册中心契约、成员状态、租约生命周期和可路由成员视图；不绑定具体后端 |
| `nano-contrib`（独立 module） | 可选 etcd、OpenTelemetry、Prometheus 适配器，不属于 nano 核心依赖 |

当前已经完成的较好设计：

- `RequestContext` 捕获每个请求自己的 MID，支持并发请求乱序回包。
- `ctx.Response` 有一次性保护，重复回包返回错误。
- Handler 返回错误、panic、无回包时会统一回系统错误。
- 内部 `ctx.Call` 能复用本地 Handler 或跨节点 gRPC。
- 系统码限制在 4 bit 范围内，避免协议头溢出。

## 修复记录

### 2026-07-26

已完成：

- 本地客户端断开后，网关节点会从 `Node.sessions` 删除当前 session。
- `agent.write` 删除内部 `chWrite` 中转队列，避免同 goroutine 自我阻塞；socket 写入增加 write deadline。
- 框架跨节点 gRPC 调用统一增加 `RPCTimeout`。
- 新增 `WithRPCTimeout`、`WithWriteTimeout`、`WithClusterAuthToken`。
- 集群 gRPC server 新增 token interceptor；配置 `WithClusterAuthToken` 后，所有节点间调用必须携带相同 token。
- message decode 增加畸形压缩路由和未终止 message id 的边界检查。
- packet encode 增加 `MaxPacketSize` 上限检查。
- session router 支持按下线节点地址清理旧绑定。
- scheduler 新增 `Stats()` 基础观测快照。
- WebSocket server 改为节点私有 `ServeMux` 和 `http.Server`，Shutdown 时可关闭。
- README 和入门文档中的旧 `*session.Session` Handler 示例已更新为 `*session.RequestContext`。
- gameServer 已接入 `NANO_CLUSTER_AUTH_TOKEN`，生产节点配置同一个环境变量即可启用集群 token 鉴权。
- 新增 `registry.Registry`、`registry.Runtime` 和 `registry.View`，注册中心契约从业务工程下沉到框架。
- 新增 `WithUnaryServerInterceptors`、`WithUnaryClientInterceptors`，集群认证、Trace、指标可以通过 gRPC interceptor 组合。
- etcd、OpenTelemetry 和 Prometheus 实现移入独立的 `nano-contrib` module，nano 核心不直接增加基础设施依赖。
- gameServer 的跨节点 gRPC 已接入 W3C Trace Context 注入和提取；业务 span 和业务指标仍由业务工程维护。

仍需部署侧配合：

- 生产集群必须配置 `NANO_CLUSTER_AUTH_TOKEN` 或显式调用 `WithClusterAuthToken`，否则框架保持兼容模式，不强制鉴权。
- 生产集群仍应优先使用内网 ServiceAddr，必要时继续接入 mTLS。
- gameServer 应显式配置 `WithScheduler(workers, backlog)`，不要依赖默认队列容量。
- etcd 当前只负责注册、租约、节点热退休和 readiness；nano master 仍负责真实成员分发和跨节点路由，不能直接停用 master。
- `nano` 与 `nano-contrib` 尚未发布包含新接口的正式版本前，工作区通过 `replace` 使用本地 module；正式发版必须先发布 nano，再发布 nano-contrib。

## 主要问题

### P0：本地 Session 没有在客户端断开时从节点表删除

位置：

- `cluster/handler.go:272-309`
- `cluster/node.go:304-307`
- `cluster/node.go:510-519`

现状：

- 客户端连接建立时，`handle` 调用 `storeSession` 放入 `Node.sessions`。
- 客户端断线时，`handle` 的 defer 只通知远端节点 `SessionClosed`，然后调用 `agent.Close()`。
- 当前节点自己的 `Node.sessions` 没有删除该 session。
- `SessionClosed` 只处理远端节点收到的关闭通知。

影响：

- 高连接 churn 时，网关节点会持续保存已断开的 session。
- 后续 Push / Response 可能命中过期 session，形成异常日志和无效工作。
- 10w 级在线场景下，断线重连会让内存占用持续增长。

修复建议：

- 增加 `Node.removeSession(sessionID)`。
- 在 `LocalHandler.handle` 的 defer 中先删除本地 session，再通知远端节点。
- 对 `agent.Close` 和远端 `SessionClosed` 的生命周期回调做幂等保护，避免本地删除和远端关闭重复触发业务 `OnClosed`。
- 增加断线单测：建立 session -> 触发 handle 退出 -> 验证当前节点 session map 删除。

### P0：写协程内部 `chWrite` 可能自我阻塞，慢连接会卡死写循环

位置：

- `cluster/agent.go:234-292`

现状：

- `agent.write` 在同一个 goroutine 内既向 `chWrite` 发送，也从 `chWrite` 接收。
- 当 `chWrite` 缓冲区满时，`chWrite <- p` 或 `chWrite <- hbd` 会阻塞。
- 因为发送和接收在同一个 goroutine，阻塞后该 goroutine 无法继续消费 `chWrite`，也无法响应 `chDie`。
- `conn.Write` 没有写超时，慢客户端也可能永久阻塞。

影响：

- 单个慢连接可能卡住自己的写循环，导致 session 无法正常关闭。
- 大量慢连接会积累 goroutine、session、发送缓冲和内存。
- Push / Response 在发送队列满时会大量失败或阻塞。

修复建议：

- 删除内部 `chWrite`，写协程直接串行编码并写 socket。
- 或拆成明确的 encoder 队列和 writer goroutine，但必须有 backpressure 和关闭路径。
- 每次 `conn.Write` 前设置 `SetWriteDeadline`，超时后关闭 session。
- `send` 改为非阻塞或带 context 的发送，不要用 `len(chSend)` 预判后再阻塞写入。
- 增加慢写连接测试：模拟阻塞 `net.Conn.Write`，验证 session 能在超时后关闭。

### P0：跨节点 gRPC 调用缺少 deadline，网关读循环和后台调用可能被拖死

位置：

- `cluster/handler.go:468-488`
- `cluster/acceptor.go:23-35`
- `cluster/acceptor.go:55-72`
- `cluster/acceptor.go:76-82`
- `cluster/node.go:186`
- `cluster/node.go:549`

现状：

- 多数跨节点调用使用 `context.Background()`。
- `remoteProcess` 在客户端读消息路径上直接调用 `HandleRequest` / `HandleNotify`。
- 如果目标节点或网络卡住，当前客户端的读循环会被阻塞。

影响：

- 远端节点慢或 gRPC 半开时，网关 session 读循环停止处理后续包和心跳。
- 级联故障时，单个远端服务异常会拖慢大量网关连接。
- 请求超时只覆盖本地 Handler 上下文，不覆盖网关转发阶段。

修复建议：

- 增加 `WithRPCTimeout` / `WithClusterCallTimeout`。
- 所有 gRPC 调用使用 `context.WithTimeout`。
- `remoteProcess` 不应在读循环中做无界阻塞调用；至少加 deadline，必要时投递到独立转发 worker。
- 注册、心跳、SessionClosed、Push、Response 分别使用不同超时和重试策略。

### P0：集群 gRPC 默认无鉴权，内部接口可被伪造调用

位置：

- `internal/env/env.go:52`
- `cluster/node.go:383-495`
- `cluster/cluster.go`

现状：

- 默认 gRPC DialOption 是 `grpc.WithInsecure()`。
- Member/Master gRPC 方法没有 mTLS、token、签名或来源校验。
- 转发身份只依赖 metadata 中的 `uid`，而 metadata 可由任意 gRPC 调用方伪造。

影响：

- 如果服务端口暴露在不可信网络，攻击者可以伪造 `HandleRequest`、`HandleResponse`、`HandlePush`、`CloseSession`。
- 可造成越权绑定 UID、伪造资产/业务回包、踢人、注册恶意节点。

修复建议：

- 默认关闭明文 gRPC，提供 `WithClusterTLS` / `WithClusterAuthToken`。
- Member/Master 加 unary interceptor，验证节点身份和权限。
- 转发身份不要只相信 `uid` metadata；加入网关 session 签名或内部 session token。
- 文档明确要求 ServiceAddr 只绑定内网地址，不得暴露公网。

### P1：全局 Scheduler 是共享瓶颈，缺少观测和隔离

位置：

- `scheduler/scheduler.go:49-56`
- `scheduler/scheduler.go:82-129`
- `cluster/handler.go:607-613`

现状：

- 默认 `chTasks` 缓冲 256。
- 默认 worker 数是 `GOMAXPROCS`。
- 所有本地 Handler 共享一个全局队列。
- 队列满时 Request 直接回 `CodeServerBusy`，Notify 被丢弃。
- 没有队列长度、入队失败、排队耗时、执行耗时指标。

影响：

- 慢接口会占住全局 worker，影响无关接口。
- 瞬时请求峰值容易产生 `ServerBusy`。
- 线上无法判断瓶颈是队列、CPU、DB、网络还是某个 Handler。

修复建议：

- 增加 scheduler metrics：`queue_len`、`enqueue_failed_total`、`task_wait_seconds`、`task_run_seconds`。
- gameServer 必须显式配置 `WithScheduler(workers, backlog)`。
- 为 Service 支持独立 worker pool，而不是只支持 session 内自定义 `LocalScheduler`。
- 慢业务强制改成“同步受理 + 后台执行 + Push 结果”。

### P1：HTTP WebSocket 使用默认全局 mux，无法优雅关闭且多节点同进程易冲突

位置：

- `cluster/node.go:260-300`

现状：

- `listenAndServeWS` / `listenAndServeWSTLS` 使用 `http.HandleFunc` 和 `http.ListenAndServe`。
- 使用默认全局 `http.DefaultServeMux`。
- `Node.Shutdown` 没有关闭 HTTP server。

影响：

- 同进程启动多个 Nano WS 节点会 route 冲突。
- 单元测试和嵌入式场景容易污染全局 mux。
- 无法优雅关闭监听端口和已有 WS 连接。

修复建议：

- `Node` 持有 `*http.Server` 和专属 `ServeMux`。
- `Shutdown` 调用 `httpServer.Shutdown(ctx)`。
- TCP listener 也应作为字段保存并在 Shutdown 关闭。

### P1：协议 Decode 对畸形压缩路由包缺少边界检查

位置：

- `internal/message/message.go:214-223`

现状：

- 当 compressed flag 打开时，直接读取 `data[offset:offset+2]`。
- 若数据长度不足 2 字节，会发生 slice 越界 panic。

影响：

- 恶意客户端可以通过构造畸形包触发连接处理 goroutine panic。
- 当前上层不是每个 decode 路径都有 recover，存在连接级崩溃风险。

修复建议：

- 在读取压缩路由前检查 `offset+2 <= len(data)`。
- Varint MID 解析需要限制最大字节数，缺少终止字节时返回 `ErrWrongMessage`。
- 增加 malformed message fuzz / table tests。

### P1：Packet Encode 不限制最大包体

位置：

- `internal/codec/codec.go:114-125`

现状：

- Decode 限制 `MaxPacketSize = 64 * 1024`。
- Encode 没有对 `len(data)` 做同样限制。
- 包长度字段只有 3 字节，超大 payload 会截断长度字段。

影响：

- 服务端 Push 大消息时可能发出客户端无法正确解析的包。
- 超大编码会造成内存尖峰。

修复建议：

- `Encode` 中增加 `len(data) > MaxPacketSize` 返回 `ErrPacketSizeExcced`。
- Push/Response 层对业务 payload 大小加日志和指标。

### P1：远程服务路由缓存不会在成员删除时清理

位置：

- `cluster/handler.go:438-442`
- `session/router.go`
- `cluster/handler.go:229-246`

现状：

- `remoteProcess` 会把某个 Service 绑定到 session router。
- `delMember` 删除远端成员时只更新 `remoteServices`，没有清理所有 session router 里的旧地址。

影响：

- 节点下线或重启后，已有 session 可能继续命中旧地址。
- 之后请求会先失败一次或多次，直到业务自行清理路由。

修复建议：

- `delMember` 时遍历本节点 session，删除指向该 `ServiceAddr` 的 route。
- 更好的方式是路由表存储 service -> member version，成员变更时使旧绑定失效。
- 对请求失败的地址做熔断和重新选路。

### P2：全局 env 配置让测试和多实例嵌入变得困难

位置：

- `internal/env/env.go`
- `options.go`
- `interface.go`

现状：

- `Heartbeat`、`Serializer`、`GrpcOptions`、`CheckOrigin`、`Debug`、`Die` 等都是进程全局。
- `WithGrpcOptions` 只 append，不替换。
- `Shutdown` 直接 close 全局 `env.Die`，无法重复启动同一进程内的 Nano。

影响：

- 单元测试之间容易互相污染。
- 同进程多节点不容易隔离配置。
- 服务重启或嵌入式场景不稳定。

修复建议：

- 把 env 收敛到 `cluster.Options` 或 `NodeConfig`。
- `Die` 改为每个 `Node` 自己的 context。
- Option 尽量只作用于当前 Node，不修改全局变量。

### P2：代码中仍有旧文档和旧示例语义

位置：

- `README.md`
- `docs/get_started*.md`

现状：

- README 仍描述 Handler 第一个参数是 `*session.Session`，并示例 `session.Response(payload)`。
- 当前框架实际 Handler 参数已经是 `*session.RequestContext`，且业务应使用 `ctx.Response`。

影响：

- 新接入者会按旧 API 写代码。
- 文档和框架不一致会导致维护误判。

修复建议：

- 更新 README 和入门文档的 Handler 签名。
- 明确：业务不接触 MID，Request 只能 `ctx.Response` 一次。
- 增加异步任务推荐模式：`Response{accepted, task_id}` + `Push{task_id, result}`。

## 修复路线

### 第一阶段：稳定性止血

1. 修复本地 session 删除和生命周期幂等。
2. 重写 `agent.write`，删除自阻塞 `chWrite`，增加写 deadline。
3. 所有 gRPC 调用加 timeout。
4. 修复 message Decode 越界和 packet Encode 大小限制。

### 第二阶段：高并发支撑

1. Scheduler 增加 metrics。
2. gameServer 显式配置 scheduler worker/backlog。
3. 支持 Service 级独立 worker pool。
4. 慢接口改异步受理，减少全局 worker 阻塞。
5. 增加慢连接、慢远端节点、请求峰值压测。

### 第三阶段：集群安全与运维化

1. gRPC 加 mTLS 或内部 token interceptor。
2. Master/Member 注册加身份校验。
3. 节点变更时清理 session router 旧绑定。
4. TCP/WS listener 和 HTTP server 支持优雅关闭。
5. env 全局配置逐步迁移到 Node 级配置。

## 建议补充测试

| 测试 | 目的 |
|---|---|
| 本地客户端断开后 session map 删除 | 防止内存泄漏回归 |
| 慢写连接触发写超时关闭 | 防止写协程卡死 |
| remoteProcess 目标节点无响应 | 验证 gRPC deadline 和读循环不被永久阻塞 |
| malformed compressed route message | 防止 Decode panic |
| Encode 超过 MaxPacketSize | 防止发出非法包 |
| member del 后 session router 重选路 | 防止旧地址缓存 |
| scheduler 队列满场景 | 验证 Request 回 ServerBusy，Notify 可丢弃 |
| 未鉴权 gRPC 调用被拒绝 | 防止内部接口暴露 |

## 结论

当前 Nano 的基础模型适合中小规模游戏服务，并且请求 MID 改造后已经解决了并发乱序回包的关键正确性问题。

但要支撑 10w 级在线或更高峰值请求，优先级最高的不是继续加业务功能，而是先修复连接生命周期、写循环阻塞、跨节点调用超时和集群鉴权。这四类问题不解决，压测结果会不稳定，线上也容易出现内存增长、连接卡死、级联超时和内部接口风险。
