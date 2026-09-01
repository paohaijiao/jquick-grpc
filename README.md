<div align="center">

# JQuick gRPC

**JQuick 生态的 gRPC 组件 —— 服务端快速发布 gRPC 服务，客户端调用一站式封装**

[![Maven Central](https://img.shields.io/maven-central/v/io.github.paohaijiao/jquick-grpc.svg?label=Maven%20Central)](https://central.sonatype.com/artifact/io.github.paohaijiao/jquick-grpc)
[![Build Status](https://github.com/paohaijiao/jquick-grpc/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/paohaijiao/jquick-grpc/actions)
[![Code Coverage](https://codecov.io/gh/paohaijiao/jquick-grpc/branch/main/graph/badge.svg)](https://codecov.io/gh/paohaijiao/jquick-grpc)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](./LICENSE)
[![JDK](https://img.shields.io/badge/JDK-8%2B-orange.svg)](./README.md#环境要求)
[![gRPC](https://img.shields.io/badge/gRPC-1.80.0-green.svg)](https://github.com/grpc/grpc-java)
[![English Docs](https://img.shields.io/badge/Docs-English-blueviolet.svg)](./README_EN.md)

</div>

---

## 简介

`jquick-grpc` 是 [JQuick](https://github.com/paohaijiao) 生态下的 gRPC 组件，基于 [grpc-java](https://github.com/grpc/grpc-java) 与 Netty 构建：

- **服务端**：一个配置类 + 一行注册，即可发布 gRPC 服务，内置健康检查（gRPC Health Checking Protocol）与反射服务；
- **客户端**：对 Stub 创建、服务发现、负载均衡、连接池、超时（Deadline）与重试做了统一封装，业务代码直接使用 gRPC 原生 BlockingStub / Stub，**零侵入、无代理接口约定**；
- **可插拔**：服务发现（Local / Nacos / Etcd）、负载均衡（轮询 / 随机 / 加权 / 最少连接）均可替换或自定义。

> 设计原则：不重复造轮子。jquick-grpc 保留 grpc-java 全部原生用法（ImplBase、Stub、Channel、Interceptor），只在生命周期、注册发现、负载均衡与资源管理等"工程化"层面做增强。

## 特性

| 特性 | 说明 |
| --- | --- |
| 服务端快速发布 | `JQuickGrpcNettyServer` / `JQuickGrpcInProcessServer`，统一 start/stop 生命周期管理 |
| 健康检查 | 内置 gRPC Health Checking Protocol，服务注册/注销自动同步 SERVING 状态 |
| 客户端封装 | 池化客户端（`Pooled`）与单连接客户端（`Single`）两种模式，支持原生 Stub 直接调用 |
| 服务发现 | 内置 Local（内存）、Nacos、Etcd 实现，SPI 式扩展 `JQuickGrpcServiceDiscovery` |
| 负载均衡 | 轮询、随机、加权（随机加权 / 平滑加权轮询）、最少连接，支持自定义与运行时切换 |
| 按调用路由 | `JQuickGrpcRoutingChannel` 在每次 RPC 发起时重新选择实例，天然支持故障转移 |
| 超时控制 | 全局 Deadline + 单调用级覆盖（`getServiceWithDeadline`） |
| 上下文传播 | 内置 traceId / userId 拦截器，客户端到服务端全链路透传 |
| 连接池 | 基于 commons-pool2 的 Channel 池化复用，可配置最大连接数与空闲策略 |
| 进程内服务器 | `JQuickGrpcInProcessServer` 零端口启动，单元测试友好 |
| 压缩 | 支持 gzip 消息压缩（`enableCompression`） |

## 环境要求

| 依赖 | 版本要求 |
| --- | --- |
| JDK | 8+（11+ 推荐） |
| grpc-java | 1.80.0 |
| JQuick（javelin） | 1.8.1 |
| 注册中心（可选） | Nacos 2.x / Etcd v3（按需引入对应 discovery 实现的依赖） |

## 快速开始

### 1. 定义 Proto 并生成代码

使用标准 protobuf 与 grpc-java 插件生成 `GreeterGrpc` / `GreeterProto`（本项目测试目录含完整示例）。

### 2. 服务端发布 gRPC 服务

```java
// 1. 实现生成的 ImplBase，可选 @JQuickGrpcService 标注服务名与版本
@JQuickGrpcService(name = "Greeter", version = 1)
public class GreeterServiceImpl extends GreeterGrpc.GreeterImplBase {
    @Override
    public void sayHello(GreeterProto.HelloRequest request,
                         StreamObserver<GreeterProto.HelloReply> responseObserver) {
        GreeterProto.HelloReply reply = GreeterProto.HelloReply.newBuilder()
                .setMessage("Hello, " + request.getName() + "!")
                .build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }
}

// 2. 配置并启动服务端
JQuickGrpcServerConfig config = new JQuickGrpcServerConfig();
config.setPort(9090);                 // 传 0 表示随机端口，启动后经 getPort() 获取实际端口
JQuickGrpcNettyServer server = new JQuickGrpcNettyServer(config);
server.registerService(new GreeterServiceImpl());   // 注册即同步健康检查状态
server.start();

// 3. 停止（优雅关闭，JVM 关闭钩子已自动注册）
server.stop();
```

### 3. 客户端调用

```java
// 1. 服务发现（生产环境使用 Nacos/Etcd，本地测试可用内存实现）
JQuickGrpcLocalDiscovery discovery = new JQuickGrpcLocalDiscovery();
discovery.registerService("GreeterService", "127.0.0.1", 9090);

// 2. 创建客户端：pooled（连接池）或 single（单连接长连）
JQuickGrpcClientConfig config = new JQuickGrpcClientConfig();
config.setClientType("pooled");
config.setDeadlineMillis(5000);       // 全局超时
JQuickGrpcClient client = new JQuickGrpcPooledClient(config, discovery,
        new JQuickGrpcRoundRobinLoadBalancer());

// 3. 拿到 gRPC 原生 BlockingStub 直接调用，无需任何代理接口
GreeterGrpc.GreeterBlockingStub stub =
        client.getService(GreeterGrpc.GreeterBlockingStub.class, "GreeterService");
GreeterProto.HelloReply reply = stub.sayHello(
        GreeterProto.HelloRequest.newBuilder().setName("JQuick").build());
System.out.println(reply.getMessage());   // Hello, JQuick!

// 4. 用完关闭，统一释放 Channel 资源
client.close();
```

也支持通过工厂统一创建与切换组件：

```java
JQuickGrpcDynamicFactory factory = new JQuickGrpcDynamicFactory();
JQuickGrpcServer server = factory.createServer(serverConfig);
JQuickGrpcClient client = factory.createClient(clientConfig, discovery,
        factory.switchLoadBalancer("weighted"));   // roundRobin / random / weighted / leastConnection
```

### 4. 拦截器与上下文传播

```java
// 服务端：start 之前注册拦截器，自动解析 traceId / userId
server.addInterceptor(new JQuickGrpcServerInterceptor());

// 客户端：业务代码中设置上下文，随每次调用自动透传到服务端
try (JQuickGrpcContext.Scope scope = JQuickGrpcContext.create()
        .withTraceId("trace-123")
        .withUserId("user-42")
        .attach()) {
    stub.sayHello(request);
}

// 服务端业务线程内读取
String traceId = JQuickGrpcContext.getTraceId();
```

> `JQuickGrpcClientInterceptor` 已由客户端自动挂载，无需手动注册。

## 依赖引入

### Maven

```xml
<dependency>
    <groupId>io.github.paohaijiao</groupId>
    <artifactId>jquick-grpc</artifactId>
    <version>1.4.0</version>
</dependency>
```

### Gradle

```groovy
implementation 'io.github.paohaijiao:jquick-grpc:1.4.0'
```

### Gradle Kotlin DSL

```kotlin
implementation("io.github.paohaijiao:jquick-grpc:1.4.0")
```

> 按需使用 Nacos / Etcd 服务发现时，请同时引入 `nacos-client` 或 `jetcd-core`（本组件已内置传递依赖，亦可用 exclude 排除后自选版本）。

## 高级用法

### 超时配置

```java
JQuickGrpcClientConfig config = new JQuickGrpcClientConfig();
config.setDeadlineMillis(5000);   // 全局默认 Deadline

// 单次调用覆盖全局 Deadline
GreeterGrpc.GreeterBlockingStub fast =
        client.getServiceWithDeadline(GreeterGrpc.GreeterBlockingStub.class, "GreeterService", 300);
```

超时触发后抛出 `StatusRuntimeException`，状态码为 `DEADLINE_EXCEEDED`，建议结合重试与熔断使用。

### 负载均衡

内置四种实现，均实现 `JQuickGrpcLoadBalancer` 接口：

| 实现 | 名称 | 说明 |
| --- | --- | --- |
| `JQuickGrpcRoundRobinLoadBalancer` | `RoundRobin` | 顺序轮询 |
| `JQuickGrpcRandomLoadBalancer` | `Random` | 随机选择 |
| `JQuickGrpcWeightedLoadBalancer` | `Weighted(RANDOM/SMOOTH_RR)` | 加权，支持普通加权随机与 Nginx 式平滑加权轮询 |
| `JQuickGrpcLeastConnectionLoadBalancer` | `LeastConnection` | 最少活跃连接，自动感知实例上下线 |

```java
// 自定义负载均衡器并注册到工厂
factory.registerLoadBalancer("myPolicy", instances -> instances.get(0));
factory.switchLoadBalancer("myPolicy");
```

不健康实例在所有实现中均会被自动过滤。

### TLS

```java
JQuickGrpcServerConfig config = JQuickGrpcServerConfig.secure(9090);  // usePlaintext = false
config.setCertChainFilePath("certs/server.pem");
config.setPrivateKeyFilePath("certs/server.key");
```

开启 TLS 但未配置证书时，服务端会在 `start()` 阶段快速失败并给出明确错误信息。

### 与 JQuick-Gateway 整合

`jquick-grpc` 面向网关场景做了两点适配：

1. **服务自动注册**：服务端启动后将服务实例注册到 Nacos / Etcd，JQuick-Gateway 通过同一注册中心发现上游 gRPC 节点并按服务名路由；
2. **协议元数据**：服务端默认开启 gRPC Server Reflection（`ProtoReflectionService`），网关的 gRPC-JSON 转码与动态方法发现可直接依赖该能力，无需静态 proto 描述文件。

> 生产环境如对反射服务有安全顾虑，可通过配置关闭或替换。

## 常见问题（FAQ）

**Q1: 客户端抛 `UNIMPLEMENTED: Method xxx is unimplemented`？**
服务端启动时未注册对应服务，或客户端调用的服务名与 proto 全限定名不一致。`server.registerService(bindableService)` 以 proto 服务全名（如 `com.example.Greeter`）作为注册键，排查两端的 service descriptor 是否一致。

**Q2: `DEADLINE_EXCEEDED` 排查思路？**
依次确认：全局/单调用 Deadline 是否设置过小 → 实例是否可达（`UNAVAILABLE` 会先于超时出现）→ 服务端是否真的处理慢。可用 `client.getStats()` 查看当前 Channel 与实例状态。

**Q3: 服务端配置 `port=0` 后如何获取真实端口？**
启动后调用 `server.getPort()`，返回操作系统实际分配的端口；也可通过 `server.getServer().getPort()`（原生 `io.grpc.Server#getPort`）获取。

**Q4: 为什么 `getService` 传入 Stub 类而不是接口？**
gRPC 生成的 Stub 是抽象类而非接口，无法使用 JDK 动态代理。jquick-grpc 通过绑定路由 Channel 的真实 Stub 实现按调用级负载均衡，同时保留原生 API。历史版本的接口代理用法（`getService` 传入接口）仍向后兼容。

**Q5: 拦截器中设置的 traceId，业务线程里为什么取不到？**
gRPC 服务端回调运行在与拦截器不同的线程上。本组件的服务端拦截器已在 `onMessage` / `onHalfClose` 等回调中自动重新 attach 上下文，请确认使用的是内置 `JQuickGrpcServerInterceptor`，并在回调内读取 `JQuickGrpcContext`。

**Q6: 如何接入自己的注册中心？**
实现 `JQuickGrpcServiceDiscovery` 接口（`getInstances` / `subscribe` / `unsubscribe` / `close`），再把实现实例传入客户端构造函数即可；`JQuickGrpcLocalDiscovery` 是最简单的参考实现。

**Q7: 集成测试为何被 `@Disabled`？**
`etcd` / `nacos` 目录下的测试依赖外部注册中心实例，为避免 CI 与本地环境无服务可连导致失败，默认禁用；移除 `@Disabled` 并配置可用地址后即可运行。

## 项目结构

```
com.github.paohaijiao.grpc
├── annotation        # @JQuickGrpcService 服务标注
├── client            # 客户端接口与 Pooled/Single 实现、路由 Channel、Stub 工厂
├── config            # 客户端 / 服务端配置
├── context           # 调用上下文（traceId / userId）
├── discovery         # 服务发现抽象与 Local/Nacos/Etcd 实现
├── factory           # 服务端工厂与 InProcess 服务器
├── health            # 健康检查管理
├── interceptor       # 客户端 / 服务端拦截器
├── loadbalance       # 负载均衡抽象与四种内置实现
├── metadata          # 实例指标元数据
├── pool              # gRPC Channel 连接池
├── resolver          # 基于服务发现的 gRPC NameResolver
└── server            # 服务端抽象与 Netty 实现
```

## 项目生态

| 项目 | 说明 |
| --- | --- |
| [javelin](https://github.com/paohaijiao/javelin) | JQuick 基础框架（父 POM 与核心能力） |
| [jquick-gateway](https://github.com/paohaijiao/jquick-gateway) | JQuick 网关，支持 gRPC 上游路由与协议转码 |
| [jquick-banner](https://github.com/paohaijiao/jquick-banner) | JQuick 启动 Banner 组件 |

## 贡献

欢迎提交 Issue 与 Pull Request。提交代码请确保：

1. 遵循现有代码风格（Apache License 2.0 文件头、中英双语注释）；
2. 新增/变更代码附带对应单元测试（`mvn test` 全绿）；
3. 依赖外部中间件的集成测试统一标注 `@Disabled`。

## 开源协议

本项目基于 [Apache License 2.0](./LICENSE) 开源。

```
Copyright [2025-2099] Martin (goudingcheng@gmail.com)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
```
