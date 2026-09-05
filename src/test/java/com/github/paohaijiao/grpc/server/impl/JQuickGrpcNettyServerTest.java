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
package com.github.paohaijiao.grpc.server.impl;

import com.github.paohaijiao.grpc.config.JQuickGrpcServerConfig;
import com.github.paohaijiao.grpc.test.GreeterGrpc;
import com.github.paohaijiao.service.JQuickGreeterServiceImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * JQuickGrpcNettyServer lifecycle unit tests (uses ephemeral port 0, no external dependencies)
 */
@DisplayName("JQuickGrpcNettyServer 生命周期 / lifecycle")
class JQuickGrpcNettyServerTest {

    private JQuickGrpcNettyServer server;

    @BeforeEach
    void setUp() {
        JQuickGrpcServerConfig config = new JQuickGrpcServerConfig();
        config.setPort(0);// 端口 0 = 随机可用端口 / port 0 = ephemeral port
        server = new JQuickGrpcNettyServer(config);
    }

    @AfterEach
    void tearDown() {
        server.stop();
    }

    @Test
    @DisplayName("启动后 running=true，停止后 running=false / running flag follows start/stop")
    void startAndStopLifecycle() throws Exception {
        server.registerService(new JQuickGreeterServiceImpl());
        server.start();
        assertThat(server.isRunning()).isTrue();
        assertThat(server.getServer()).isNotNull();
        assertThat(server.getServer().isShutdown()).isFalse();
        assertThat(server.getServer().getPort()).isPositive();// 实际绑定端口 / actual bound port

        server.stop();
        assertThat(server.isRunning()).isFalse();
        assertThat(server.getServer().isShutdown()).isTrue();
    }

    @Test
    @DisplayName("重复 start 幂等 / start is idempotent")
    void startIsIdempotent() throws Exception {
        server.registerService(new JQuickGreeterServiceImpl());
        server.start();
        int port = server.getServer().getPort();
        server.start();//  second start is a no-op
        assertThat(server.isRunning()).isTrue();
        assertThat(server.getServer().getPort()).isEqualTo(port);
    }

    @Test
    @DisplayName("未 start 直接 stop 安全 / stop without start is safe")
    void stopWithoutStartIsSafe() {
        assertThatCode(server::stop).doesNotThrowAnyException();
        assertThat(server.isRunning()).isFalse();
    }

    @Test
    @DisplayName("stop 后可重新 start / restart after stop")
    void restartAfterStop() throws Exception {
        server.registerService(new JQuickGreeterServiceImpl());
        server.start();
        server.stop();

        server.start();
        assertThat(server.isRunning()).isTrue();
        assertThat(server.getServer().isShutdown()).isFalse();

        server.stop();
        assertThat(server.isRunning()).isFalse();
    }

    @Test
    @DisplayName("服务注册与注销 / register and unregister services")
    void registerAndUnregisterService() {
        String serviceName = "Greeter";
        server.registerService(new JQuickGreeterServiceImpl());
        assertThat(server.getRegisteredServices()).containsKey(serviceName);

        server.unregisterService(serviceName);
        assertThat(server.getRegisteredServices()).doesNotContainKey(serviceName);
    }

    @Test
    @DisplayName("开启 TLS 但未配置证书时快速失败 / fail fast when TLS enabled without certificates")
    void tlsWithoutCertificatesFailsFast() {
        JQuickGrpcServerConfig secureConfig = JQuickGrpcServerConfig.secure(0);
        JQuickGrpcNettyServer secureServer = new JQuickGrpcNettyServer(secureConfig);
        assertThatThrownBy(secureServer::start)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("TLS")
                .hasMessageContaining("certChainFile");
        assertThat(secureServer.isRunning()).isFalse();// 失败后 running 保持 false
    }

    @Test
    @DisplayName("getPort 返回配置端口 / getPort returns configured port")
    void getPortReturnsConfiguredPort() {
        assertThat(server.getPort()).isZero();
    }
}
