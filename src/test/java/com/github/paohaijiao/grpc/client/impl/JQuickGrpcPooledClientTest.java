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
import io.grpc.BindableService;
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
 * <p>
 * JQuickGrpcPooledClient unit tests:
 * - discovery is backed by the in-memory implementation, no external registry required
 * - the network layer talks to a real Netty gRPC server on localhost with an ephemeral port
 */
@DisplayName("JQuickGrpcPooledClient 池化客户端 / pooled client")
class JQuickGrpcPooledClientTest {

    private JQuickGrpcLocalDiscovery discovery;

    private JQuickGrpcPooledClient client;

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
        config.setDeadlineMillis(5000);
        config.setMaxRetries(1);
        config.setRetryDelayMillis(0);
        return config;
    }

    /**
     * 启动一个挂在随机端口上的 Netty gRPC 服务并返回实际端口。
     * Starts a Netty gRPC server on an ephemeral port and returns the actual port.
     */
    private JQuickGrpcNettyServer startServer(BindableService service) throws Exception {
        JQuickGrpcServerConfig config = new JQuickGrpcServerConfig();
        config.setPort(0);
        JQuickGrpcNettyServer server = new JQuickGrpcNettyServer(config);
        server.registerService(service);
        server.start();
        return server;
    }

    private static GreeterProto.HelloRequest request(String name) {
        return GreeterProto.HelloRequest.newBuilder().setName(name).build();
    }

    @Test
    @DisplayName("客户端类型与初始统计 / client type and initial stats")
    void clientTypeAndStats() {
        client = new JQuickGrpcPooledClient(clientConfig(), discovery, new JQuickGrpcRoundRobinLoadBalancer());
        assertThat(client.getClientType()).isEqualTo("pooled");
        assertThat(client.isClosed()).isFalse();
        assertThat(client.getStats()).containsEntry("closed", false);
    }

    @Test
    @DisplayName("构造参数空值校验 / constructor rejects null config and load balancer")
    void constructorRejectsNullArguments() {
        assertThatThrownBy(() -> new JQuickGrpcPooledClient(null, discovery, new JQuickGrpcRoundRobinLoadBalancer()))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new JQuickGrpcPooledClient(clientConfig(), discovery, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("getService 参数空值校验 / getService rejects null arguments")
    void getServiceRejectsNullArguments() {
        client = new JQuickGrpcPooledClient(clientConfig(), discovery, new JQuickGrpcRoundRobinLoadBalancer());
        assertThatThrownBy(() -> client.getService(null, "svc"))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> client.getService(GreeterGrpc.GreeterBlockingStub.class, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("无可用实例时快速失败 / fails fast when no instance is available")
    void failsFastWithoutInstances() {
        client = new JQuickGrpcPooledClient(clientConfig(), discovery, new JQuickGrpcRoundRobinLoadBalancer());
        GreeterGrpc.GreeterBlockingStub stub = client.getService(GreeterGrpc.GreeterBlockingStub.class, "MissingService");
        assertThatThrownBy(() -> stub.sayHello(request("x")))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("No available service instance")
                .hasMessageContaining("MissingService");
    }

    @Test
    @DisplayName("不健康实例被过滤 / unhealthy instances are filtered out")
    void unhealthyInstancesAreFilteredOut() {
        discovery.registerService("GreeterService", "localhost", 9099);
        discovery.updateHealth("GreeterService", "localhost", 9099, false);
        client = new JQuickGrpcPooledClient(clientConfig(), discovery, new JQuickGrpcRoundRobinLoadBalancer());
        GreeterGrpc.GreeterBlockingStub stub = client.getService(GreeterGrpc.GreeterBlockingStub.class, "GreeterService");
        assertThatThrownBy(() -> stub.sayHello(request("x")))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("No available service instance");
    }

    @Test
    @DisplayName("端到端阻塞调用（真实 Netty 服务） / end-to-end blocking call against a real Netty server")
    void unaryCallEndToEnd() throws Exception {
        JQuickGrpcNettyServer server = startServer(new JQuickGreeterServiceImpl());
        try {
            int port = server.getServer().getPort();
            discovery.registerService("GreeterService", "localhost", port);
            client = new JQuickGrpcPooledClient(clientConfig(), discovery, new JQuickGrpcRoundRobinLoadBalancer());
            GreeterGrpc.GreeterBlockingStub stub = client.getService(GreeterGrpc.GreeterBlockingStub.class, "GreeterService");
            assertThat(stub.sayHello(request("JQuick")).getMessage()).isEqualTo("Hello, JQuick!");
            assertThat(stub.sayHello(request("Again")).getMessage()).isEqualTo("Hello, Again!");
            assertThat(client.getStats()).containsEntry("channelPools", 1);
        } finally {
            server.stop();
        }
    }

    @Test
    @DisplayName("全局 Deadline 生效：超时返回 DEADLINE_EXCEEDED / global deadline is honored")
    void globalDeadlineIsHonored() throws Exception {
        JQuickGrpcNettyServer server = startServer(new DelayingGreeter(2000));
        try {
            discovery.registerService("GreeterService", "localhost", server.getServer().getPort());
            JQuickGrpcClientConfig config = clientConfig();
            config.setDeadlineMillis(200);
            client = new JQuickGrpcPooledClient(config, discovery, new JQuickGrpcRoundRobinLoadBalancer());

            GreeterGrpc.GreeterBlockingStub stub = client.getService(GreeterGrpc.GreeterBlockingStub.class, "GreeterService");
            assertThatThrownBy(() -> stub.sayHello(request("x")))
                    .isInstanceOf(StatusRuntimeException.class)
                    .hasMessageContaining(Status.Code.DEADLINE_EXCEEDED.name());
        } finally {
            server.stop();
        }
    }

    @Test
    @DisplayName("getServiceWithDeadline 覆盖全局 Deadline / per-call deadline overrides the global one")
    void perCallDeadlineOverridesGlobalDeadline() throws Exception {
        JQuickGrpcNettyServer server = startServer(new DelayingGreeter(2000));
        try {
            discovery.registerService("GreeterService", "localhost", server.getServer().getPort());
            // 全局 deadline 很长，单次调用 deadline 很短 / long global deadline, short per-call deadline
            JQuickGrpcClientConfig config = clientConfig();
            config.setDeadlineMillis(60000);
            client = new JQuickGrpcPooledClient(config, discovery, new JQuickGrpcRoundRobinLoadBalancer());

            GreeterGrpc.GreeterBlockingStub shortDeadline =
                    client.getServiceWithDeadline(GreeterGrpc.GreeterBlockingStub.class, "GreeterService", 200);
            assertThatThrownBy(() -> shortDeadline.sayHello(request("x")))
                    .isInstanceOf(StatusRuntimeException.class)
                    .hasMessageContaining(Status.Code.DEADLINE_EXCEEDED.name());
        } finally {
            server.stop();
        }
    }

    @Test
    @DisplayName("getServiceWithDeadline 正常场景 / per-call deadline with a healthy service")
    void perCallDeadlineSucceedsNormally() throws Exception {
        JQuickGrpcNettyServer server = startServer(new JQuickGreeterServiceImpl());
        try {
            discovery.registerService("GreeterService", "localhost", server.getServer().getPort());
            client = new JQuickGrpcPooledClient(clientConfig(), discovery, new JQuickGrpcRoundRobinLoadBalancer());
            GreeterGrpc.GreeterBlockingStub stub =
                    client.getServiceWithDeadline(GreeterGrpc.GreeterBlockingStub.class, "GreeterService", 5000);
            assertThat(stub.sayHello(request("deadline")).getMessage()).isEqualTo("Hello, deadline!");
        } finally {
            server.stop();
        }
    }

    @Test
    @DisplayName("目标实例不可达时重试后抛出 UNAVAILABLE / unreachable instance retries then fails with UNAVAILABLE")
    void unreachableInstanceFailsWithUnavailable() throws Exception {
        int closedPort = findClosedPort();
        discovery.registerService("GreeterService", "localhost", closedPort);
        client = new JQuickGrpcPooledClient(clientConfig(), discovery, new JQuickGrpcRoundRobinLoadBalancer());
        GreeterGrpc.GreeterBlockingStub stub = client.getService(GreeterGrpc.GreeterBlockingStub.class, "GreeterService");
        assertThatThrownBy(() -> stub.sayHello(request("x")))
                .isInstanceOf(StatusRuntimeException.class)
                .hasMessageContaining(Status.Code.UNAVAILABLE.name());
    }

    @Test
    @DisplayName("close 幂等且关闭后拒绝新调用 / close is idempotent and rejects further calls")
    void closeIsIdempotentAndBlocksNewCalls() {
        client = new JQuickGrpcPooledClient(clientConfig(), discovery, new JQuickGrpcRoundRobinLoadBalancer());
        client.close();
        client.close();
        assertThat(client.isClosed()).isTrue();
        assertThat(client.getStats()).containsEntry("closed", true);
        assertThatThrownBy(() -> client.getService(GreeterGrpc.GreeterBlockingStub.class, "svc"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already closed");
    }

    /**
     * 找一个当前无人监听的端口（打开后立即关闭）。
     * Finds a port that is currently not listening (opened and immediately closed).
     */
    private static int findClosedPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            int port = socket.getLocalPort();
            return port;
        }
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
