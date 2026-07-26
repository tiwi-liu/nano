# ADR-001：服务治理采用核心契约与可选适配器分层

## 状态

Accepted

## 日期

2026-07-26

## 背景

链路追踪、etcd 注册、Prometheus 健康检查和 Grafana 部署最初全部位于 gameServer。新建 nano 服务时必须复制启动代码、注册协议和部署资源，多个服务还可能形成不同的成员模型和生命周期语义。

如果把所有实现直接加入 nano 核心，则单机服务也会依赖 etcd client、OpenTelemetry SDK 和 Prometheus，并让框架绑定具体基础设施。

## 决策

- nano 核心只提供注册中心契约、成员生命周期、路由视图和 gRPC interceptor 扩展点。
- etcd、OpenTelemetry、Prometheus 实现放入独立 `nano-contrib` module。
- 业务 span、业务指标和业务 Dashboard 留在应用工程。
- etcd、Collector、Jaeger、Prometheus 和 Grafana 作为共享部署平台维护。
- 迁移期继续由 nano master 负责真实成员分发和路由，etcd 负责注册、lease、热退休及 readiness。

## 备选方案

### 能力全部留在 gameServer

优点是 nano 不发生变化；缺点是每个新服务重复建设，公共协议无法统一。拒绝。

### 所有实现进入 nano 核心

优点是应用接入简单；缺点是核心依赖膨胀、基础设施选择被固化，单机服务也承担无关依赖。拒绝。

### 核心契约与适配器放在同一个 nano module

即使业务不 import 适配器包，module 仍需维护全部基础设施依赖和版本。独立 nano-contrib 的依赖边界更清晰，因此未采用。

## 结果

- 新服务可以复用一致的注册、Trace 和健康检查能力。
- nano 核心保持后端无关和轻量。
- nano 与 nano-contrib 必须按顺序独立发版。
- master 与 etcd 在迁移期并存，运维文档必须明确二者职责，避免误停 master。
- 后续替换 master 路由属于新的架构决策，需另建 ADR。

