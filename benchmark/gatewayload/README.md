# Gateway connection load test

该工具使用真实 Nano TCP handshake 建立连接，并保持读取服务端心跳。压测客户端每条连接只使用一个读取 goroutine，减少压测机自身开销。

## 运行

先启动单机网关或 Gate：

```bash
ulimit -n 200000
go run ./benchmark/gatewayload \
  -addr 127.0.0.1:34590 \
  -transport ws \
  -ws-path /nano \
  -connections 10000 \
  -rate 1000 \
  -parallel 256 \
  -hold 2m
```

建议逐级增加目标：

```text
10,000 → 30,000 → 50,000 → 80,000 → 100,000
```

生产容量不要取首次失败点。建议将稳定运行 30 分钟且失败率低于 0.1%、网关 CPU/内存/GC/P99 均满足目标的连接数乘以 70%，作为单机安全容量。

## 参数

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `-addr` | `127.0.0.1:34590` | 网关 TCP 地址 |
| `-transport` | `ws` | `ws` 或 `tcp` |
| `-ws-path` | `/nano` | WebSocket 服务路径 |
| `-connections` | `10000` | 目标连接数 |
| `-rate` | `1000` | 每秒连接爬升速度 |
| `-parallel` | `256` | 最大并行拨号数 |
| `-dial-timeout` | `5s` | TCP 连接和 Nano 握手超时 |
| `-hold` | `2m` | 爬升后的保持时间，`0` 等待 Ctrl+C |
| `-report` | `1s` | 指标输出间隔 |

`loadgen_heap` 和 `goroutines` 是压测进程自身指标，不是网关指标。网关应同时采集 RSS、Heap、StackInuse、Goroutines、FD、GC CPU、消息延迟以及 SessionClosed RPC 数量。

若网关与压测工具运行在同一台机器，CPU、端口、FD 和内存会互相竞争。正式容量测试应至少使用两台压测机，并从不同源 IP 发起连接，避免单机临时端口上限先成为瓶颈。
