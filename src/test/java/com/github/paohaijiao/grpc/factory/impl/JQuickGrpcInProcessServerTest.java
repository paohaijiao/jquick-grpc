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
package com.github.paohaijiao.grpc.factory.impl;

import com.github.paohaijiao.grpc.config.JQuickGrpcServerConfig;
import com.github.paohaijiao.grpc.test.GreeterGrpc;
import com.github.paohaijiao.grpc.test.GreeterProto;
import com.github.paohaijiao.service.JQuickGreeterServiceImpl;
import io.grpc.StatusRuntimeException;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.inprocess.InProcessChannelBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * JQuickGrpcInProcessServer unit tests: real in-process gRPC calls, no port or external dependencies
 */
@DisplayName("JQuickGrpcInProcessServer 进程内服务 / in-process server")
class JQuickGrpcInProcessServerTest {

    private JQuickGrpcInProcessServer server;

    @BeforeEach
    void setUp() {
        server = new JQuickGrpcInProcessServer(new JQuickGrpcServerConfig());
    }

    @AfterEach
    void tearDown() {
        server.stop();
    }

    @Test
    @DisplayName("生命周期：start/stop / lifecycle: start and stop")
    void startAndStopLifecycle() throws Exception {
        server.registerService(new JQuickGreeterServiceImpl());
        server.start();
        assertThat(server.isRunning()).isTrue();
        assertThat(server.getServer()).isNotNull();

        server.stop();
        assertThat(server.isRunning()).isFalse();
        assertThat(server.getServer().isShutdown()).isTrue();
    }

    @Test
    @DisplayName("重复 start 幂等 / start is idempotent")
    void startIsIdempotent() throws Exception {
        server.registerService(new JQuickGreeterServiceImpl());
        server.start();
        server.start();
        assertThat(server.isRunning()).isTrue();
    }

    @Test
    @DisplayName("进程内阻塞式调用返回预期结果 / unary call over in-process channel")
    void unaryCallEndToEnd() throws Exception {
        server.registerService(new JQuickGreeterServiceImpl());
        server.start();

        io.grpc.ManagedChannel channel = InProcessChannelBuilder.forName(server.getServerName()).directExecutor().build();
        try {
            GreeterGrpc.GreeterBlockingStub stub = GreeterGrpc.newBlockingStub(channel);
            GreeterProto.HelloReply reply = stub.sayHello(GreeterProto.HelloRequest.newBuilder().setName("world").build());
            assertThat(reply.getMessage()).isEqualTo("Hello, world!");
        } finally {
            channel.shutdownNow();
        }
    }

    @Test
    @DisplayName("健康检查服务返回 SERVING，注销后返回 NOT_FOUND / health check status")
    void healthCheckReflectsRegistration() throws Exception {
        server.registerService(new JQuickGreeterServiceImpl());
        server.start();

        // services are keyed by the fully qualified proto service name
        String serviceName = new JQuickGreeterServiceImpl().bindService().getServiceDescriptor().getName();
        io.grpc.ManagedChannel channel = InProcessChannelBuilder.forName(server.getServerName()).directExecutor().build();
        try {
            HealthGrpc.HealthBlockingStub healthStub = HealthGrpc.newBlockingStub(channel);
            HealthCheckResponse response = healthStub.check(HealthCheckRequest.newBuilder().setService(serviceName).build());
            assertThat(response.getStatus()).isEqualTo(HealthCheckResponse.ServingStatus.SERVING);

            server.unregisterService(serviceName);
            //  NOT_FOUND / check after clearStatus yields NOT_FOUND
            org.assertj.core.api.Assertions.assertThatThrownBy(
                            () -> healthStub.check(HealthCheckRequest.newBuilder().setService(serviceName).build()))
                    .isInstanceOf(io.grpc.StatusRuntimeException.class)
                    .hasMessageContaining("NOT_FOUND");
        } finally {
            channel.shutdownNow();
        }
    }

    @Test
    @DisplayName("getPort 始终为 0（进程内无端口）/ in-process port is always 0")
    void inProcessPortIsZero() {
        assertThat(server.getPort()).isZero();
    }

    @Test
    @DisplayName("注册表中可见已注册服务 / registered services are visible")
    void registeredServicesAreVisible() {
        //  services are keyed by the fully qualified proto service name
        String serviceName = new JQuickGreeterServiceImpl().bindService().getServiceDescriptor().getName();
        server.registerService(new JQuickGreeterServiceImpl());
        assertThat(server.getRegisteredServices()).containsKey(serviceName);
    }

    @Test
    @DisplayName("未启动服务调用将失败 / call fails before server starts")
    void callFailsBeforeStart() {
        io.grpc.ManagedChannel channel = InProcessChannelBuilder.forName("no-such-server-" + System.nanoTime()).directExecutor().build();
        try {
            GreeterGrpc.GreeterBlockingStub stub = GreeterGrpc.newBlockingStub(channel);
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> stub.sayHello(GreeterProto.HelloRequest.newBuilder().setName("x").build()))
                    .isInstanceOf(StatusRuntimeException.class);
        } finally {
            channel.shutdownNow();
        }
    }
}
