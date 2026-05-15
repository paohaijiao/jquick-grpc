package com.github.paohaijiao;

import com.github.paohaijiao.grpc.config.JQuickGrpcServerConfig;
import com.github.paohaijiao.grpc.server.impl.JQuickGrpcNettyServer;
import io.grpc.BindableService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


import static org.junit.jupiter.api.Assertions.*;

class JQuickGrpcNettyServerTest {

    private JQuickGrpcNettyServer server;
    private JQuickGrpcServerConfig config;

    @BeforeEach
    void setUp() {
        config = JQuickGrpcServerConfig.defaultConfig();
        config.setPort(0); // 随机端口
        server = new JQuickGrpcNettyServer(config);
    }

    @AfterEach
    void tearDown() {
        if (server != null && server.isRunning()) {
            server.stop();
        }
    }

    @Test
    void testStartAndStop() throws Exception {
        assertFalse(server.isRunning());
        server.start();
        assertTrue(server.isRunning());
        server.stop();
        assertFalse(server.isRunning());
    }

    @Test
    void testRegisterService() throws Exception {
        TestService testService = new TestService();
        server.registerService(testService);
        server.start();
        assertTrue(server.getRegisteredServices().containsKey("test.TestService"));
        assertEquals(1, server.getRegisteredServices().size());
    }

    @Test
    void testRegisterServiceWithName() throws Exception {
        TestService testService = new TestService();
        server.registerService("custom-service", testService);
        server.start();
        assertTrue(server.getRegisteredServices().containsKey("custom-service"));
    }

    @Test
    void testUnregisterService() throws Exception {
        TestService testService = new TestService();
        server.registerService(testService);
        server.start();
        assertTrue(server.getRegisteredServices().containsKey("test.TestService"));
        server.unregisterService("test.TestService");
        assertFalse(server.getRegisteredServices().containsKey("test.TestService"));
    }

    @Test
    void testGetPort() throws Exception {
        config.setPort(9090);
        server = new JQuickGrpcNettyServer(config);
        assertEquals(9090, server.getPort());
    }

    @Test
    void testGetHealthManager() {
        assertNotNull(server.getHealthManager());
    }

    // 测试用的简单服务
    static class TestService implements BindableService {
        @Override
        public io.grpc.ServerServiceDefinition bindService() {
            return io.grpc.ServerServiceDefinition.builder(io.grpc.ServiceDescriptor.newBuilder("test.TestService").build()
            ).build();
        }
    }
}
