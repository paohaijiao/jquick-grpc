package com.github.paohaijiao;

import com.github.paohaijiao.grpc.config.JQuickGrpcServerConfig;
import com.github.paohaijiao.grpc.factory.impl.JQuickGrpcInProcessServer;
import io.grpc.BindableService;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServiceDescriptor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class JQuickGrpcInProcessServerTest {

    private JQuickGrpcInProcessServer server;

    @BeforeEach
    void setUp() {
        JQuickGrpcServerConfig config = JQuickGrpcServerConfig.defaultConfig();
        server = new JQuickGrpcInProcessServer(config);
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
    void testGetServerName() {
        String serverName = server.getServerName();
        assertNotNull(serverName);
        assertTrue(serverName.startsWith("inprocess-server-"));
    }

    @Test
    void testGetPort() {
        assertEquals(0, server.getPort());
    }

    @Test
    void testRegisterAndUnregisterService() throws Exception {
        TestService testService = new TestService();
        server.registerService(testService);
        server.start();
        assertTrue(server.getRegisteredServices().containsKey("test.TestService"));
        server.unregisterService("test.TestService");
        assertFalse(server.getRegisteredServices().containsKey("test.TestService"));
    }

    static class TestService implements BindableService {
        @Override
        public ServerServiceDefinition bindService() {
            return ServerServiceDefinition.builder(
                    ServiceDescriptor.newBuilder("test.TestService").build()
            ).build();
        }
    }
}
