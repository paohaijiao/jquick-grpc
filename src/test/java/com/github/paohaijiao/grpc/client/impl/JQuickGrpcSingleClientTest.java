/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.paohaijiao.grpc.client.impl;

import com.github.paohaijiao.grpc.config.JQuickGrpcClientConfig;
import com.github.paohaijiao.grpc.config.JQuickGrpcServerConfig;
import com.github.paohaijiao.grpc.discovery.impl.JQuickGrpcLocalDiscovery;
import com.github.paohaijiao.grpc.loadbalance.impl.JQuickGrpcRoundRobinLoadBalancer;
import com.github.paohaijiao.grpc.server.impl.JQuickGrpcNettyServer;
import com.github.paohaijiao.grpc.test.GreeterGrpc;
import com.github.paohaijiao.grpc.test.GreeterProto;
import com.github.paohaijiao.service.JQuickGreeterServiceImpl;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * JQuickGrpcSingleClient unit tests: local discovery + real Netty server on an ephemeral port
 */
@DisplayName("JQuickGrpcSingleClient 单连接客户端 / single client")
class JQuickGrpcSingleClientTest {

    private JQuickGrpcLocalDiscovery discovery;

    private JQuickGrpcSingleClient client;

    @BeforeEach
    void setUp() {
        discovery = new JQuickGrpcLocalDiscovery();
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close();
        }
        discovery.close();
    }

    private JQuickGrpcClientConfig clientConfig() {
        JQuickGrpcClientConfig config = new JQuickGrpcClientConfig();
        config.setClientType("single");
        config.setDeadlineMillis(5000);
        config.setMaxRetries(1);
        config.setRetryDelayMillis(0);
        return config;
    }

    private JQuickGrpcNettyServer startServer() throws Exception {
        JQuickGrpcServerConfig config = new JQuickGrpcServerConfig();
        config.setPort(0);
        JQuickGrpcNettyServer server = new JQuickGrpcNettyServer(config);
        server.registerService(new JQuickGreeterServiceImpl());
        server.start();
        return server;
    }

    private static GreeterProto.HelloRequest request(String name) {
        return GreeterProto.HelloRequest.newBuilder().setName(name).build();
    }

    @Test
    @DisplayName("客户端类型与初始统计 / client type and initial stats")
    void clientTypeAndStats() {
        client = new JQuickGrpcSingleClient(clientConfig(), discovery, new JQuickGrpcRoundRobinLoadBalancer());
        assertThat(client.getClientType()).isEqualTo("single");
        assertThat(client.isClosed()).isFalse();
    }

    @Test
    @DisplayName("构造参数空值校验 / constructor rejects null arguments")
    void constructorRejectsNullArguments() {
        assertThatThrownBy(() -> new JQuickGrpcSingleClient(null, discovery, new JQuickGrpcRoundRobinLoadBalancer()))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new JQuickGrpcSingleClient(clientConfig(), discovery, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("端到端阻塞调用，且复用同一长连接 / end-to-end call reusing a single long-lived channel")
    void unaryCallEndToEnd() throws Exception {
        JQuickGrpcNettyServer server = startServer();
        try {
            discovery.registerService("GreeterService", "localhost", server.getServer().getPort());
            client = new JQuickGrpcSingleClient(clientConfig(), discovery, new JQuickGrpcRoundRobinLoadBalancer());

            GreeterGrpc.GreeterBlockingStub stub = client.getService(GreeterGrpc.GreeterBlockingStub.class, "GreeterService");
            assertThat(stub.sayHello(request("JQuick")).getMessage()).isEqualTo("Hello, JQuick!");
            assertThat(stub.sayHello(request("Again")).getMessage()).isEqualTo("Hello, Again!");

            // 单连接模式：同一实例只建一条 Channel / single mode: one channel per instance
            assertThat(client.getStats()).containsEntry("channelCount", 1);
        } finally {
            server.stop();
        }
    }

    @Test
    @DisplayName("全局 Deadline 生效 / global deadline is honored")
    void globalDeadlineIsHonored() throws Exception {
        JQuickGrpcNettyServer server = startServer();
        try {
            // 使用延迟实现验证超时 / use a delaying service to verify the deadline
            JQuickGrpcNettyServer delaying = new JQuickGrpcNettyServer(configWithRandomPort());
            delaying.registerService(new DelayingGreeter(2000));
            delaying.start();
            try {
                discovery.registerService("SlowService", "localhost", delaying.getServer().getPort());
                JQuickGrpcClientConfig config = clientConfig();
                config.setDeadlineMillis(200);
                client = new JQuickGrpcSingleClient(config, discovery, new JQuickGrpcRoundRobinLoadBalancer());
                GreeterGrpc.GreeterBlockingStub stub = client.getService(GreeterGrpc.GreeterBlockingStub.class, "SlowService");
                assertThatThrownBy(() -> stub.sayHello(request("x")))
                        .isInstanceOf(StatusRuntimeException.class)
                        .hasMessageContaining(Status.Code.DEADLINE_EXCEEDED.name());
            } finally {
                delaying.stop();
            }
        } finally {
            server.stop();
        }
    }

    @Test
    @DisplayName("getServiceWithDeadline 覆盖全局 Deadline / per-call deadline overrides the global one")
    void perCallDeadlineOverridesGlobalDeadline() throws Exception {
        JQuickGrpcNettyServer delaying = new JQuickGrpcNettyServer(configWithRandomPort());
        delaying.registerService(new DelayingGreeter(2000));
        delaying.start();
        try {
            discovery.registerService("SlowService", "localhost", delaying.getServer().getPort());
            JQuickGrpcClientConfig config = clientConfig();
            config.setDeadlineMillis(60000);
            client = new JQuickGrpcSingleClient(config, discovery, new JQuickGrpcRoundRobinLoadBalancer());
            GreeterGrpc.GreeterBlockingStub stub =
                    client.getServiceWithDeadline(GreeterGrpc.GreeterBlockingStub.class, "SlowService", 200);
            assertThatThrownBy(() -> stub.sayHello(request("x")))
                    .isInstanceOf(StatusRuntimeException.class)
                    .hasMessageContaining(Status.Code.DEADLINE_EXCEEDED.name());
        } finally {
            delaying.stop();
        }
    }

    @Test
    @DisplayName("无可用实例时快速失败 / fails fast when no instance is available")
    void failsFastWithoutInstances() {
        client = new JQuickGrpcSingleClient(clientConfig(), discovery, new JQuickGrpcRoundRobinLoadBalancer());
        GreeterGrpc.GreeterBlockingStub stub = client.getService(GreeterGrpc.GreeterBlockingStub.class, "MissingService");
        assertThatThrownBy(() -> stub.sayHello(request("x")))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("No available service instance");
    }

    @Test
    @DisplayName("目标实例不可达时抛出 UNAVAILABLE / unreachable instance fails with UNAVAILABLE")
    void unreachableInstanceFailsWithUnavailable() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            discovery.registerService("GreeterService", "localhost", socket.getLocalPort());
        }
        client = new JQuickGrpcSingleClient(clientConfig(), discovery, new JQuickGrpcRoundRobinLoadBalancer());
        GreeterGrpc.GreeterBlockingStub stub = client.getService(GreeterGrpc.GreeterBlockingStub.class, "GreeterService");
        assertThatThrownBy(() -> stub.sayHello(request("x")))
                .isInstanceOf(StatusRuntimeException.class)
                .hasMessageContaining(Status.Code.UNAVAILABLE.name());
    }

    @Test
    @DisplayName("close 幂等且关闭后拒绝新调用 / close is idempotent and rejects further calls")
    void closeIsIdempotentAndBlocksNewCalls() {
        client = new JQuickGrpcSingleClient(clientConfig(), discovery, new JQuickGrpcRoundRobinLoadBalancer());
        client.close();
        client.close();
        assertThat(client.isClosed()).isTrue();
        assertThatThrownBy(() -> client.getService(GreeterGrpc.GreeterBlockingStub.class, "svc"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already closed");
    }

    private static JQuickGrpcServerConfig configWithRandomPort() {
        JQuickGrpcServerConfig config = new JQuickGrpcServerConfig();
        config.setPort(0);
        return config;
    }

    /**
     * 延迟响应的 Greeter 实现，用于验证 Deadline 生效。
     * A Greeter implementation with delayed response, used to verify deadlines.
     */
    private static final class DelayingGreeter extends GreeterGrpc.GreeterImplBase {

        private final long delayMillis;

        private DelayingGreeter(long delayMillis) {
            this.delayMillis = delayMillis;
        }

        @Override
        public void sayHello(GreeterProto.HelloRequest request, StreamObserver<GreeterProto.HelloReply> responseObserver) {
            try {
                Thread.sleep(delayMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            responseObserver.onNext(GreeterProto.HelloReply.newBuilder().setMessage("Hello, " + request.getName() + "!").build());
            responseObserver.onCompleted();
        }
    }
}
