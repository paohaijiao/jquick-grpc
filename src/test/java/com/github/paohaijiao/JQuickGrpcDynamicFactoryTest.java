package com.github.paohaijiao;
import com.github.paohaijiao.grpc.client.JQuickGrpcClient;
import com.github.paohaijiao.grpc.config.JQuickGrpcClientConfig;
import com.github.paohaijiao.grpc.config.JQuickGrpcServerConfig;
import com.github.paohaijiao.grpc.domain.JQuickGrpcServiceInstance;
import com.github.paohaijiao.grpc.factory.JQuickGrpcDynamicFactory;
import com.github.paohaijiao.grpc.loadbalance.JQuickGrpcLoadBalancer;
import com.github.paohaijiao.grpc.server.JQuickGrpcServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class JQuickGrpcDynamicFactoryTest {

    private JQuickGrpcDynamicFactory factory;

    @BeforeEach
    void setUp() {
        factory = new JQuickGrpcDynamicFactory();
    }

    @AfterEach
    void tearDown() {
        if (factory.getActiveServer() != null && factory.getActiveServer().isRunning()) {
            factory.getActiveServer().stop();
        }
        if (factory.getActiveClient() != null && !factory.getActiveClient().isClosed()) {
            factory.getActiveClient().close();
        }
    }

    @Test
    void testCreateServer() {
        JQuickGrpcServerConfig config = JQuickGrpcServerConfig.defaultConfig();
        config.setPort(0);
        JQuickGrpcServer server = factory.createServer(config);
        assertNotNull(server);
        assertEquals(factory.getActiveServer(), server);
    }

    @Test
    void testCreatePooledClient() {
        JQuickGrpcClientConfig config = new JQuickGrpcClientConfig();
        config.setClientType("pooled");
        JQuickGrpcClient client = factory.createClient(config, null, null);
        assertNotNull(client);
        assertEquals("pooled", client.getClientType());
        assertFalse(client.isClosed());
    }

    @Test
    void testCreateSingleClient() {
        JQuickGrpcClientConfig config = new JQuickGrpcClientConfig();
        config.setClientType("single");
        JQuickGrpcClient client = factory.createClient(config, null, null);
        assertNotNull(client);
        assertEquals("single", client.getClientType());
    }

    @Test
    void testSwitchLoadBalancer() {
        JQuickGrpcLoadBalancer roundRobin = factory.switchLoadBalancer("roundRobin");
        assertNotNull(roundRobin);
        assertEquals("RoundRobin", roundRobin.getName());
        JQuickGrpcLoadBalancer random = factory.switchLoadBalancer("random");
        assertNotNull(random);
        assertEquals("Random", random.getName());
        JQuickGrpcLoadBalancer weighted = factory.switchLoadBalancer("weighted");
        assertNotNull(weighted);
    }

    @Test
    void testSwitchLoadBalancerInvalid() {
        assertThrows(IllegalArgumentException.class, () -> {
            factory.switchLoadBalancer("invalid");
        });
    }

    @Test
    void testRegisterLoadBalancer() {
        JQuickGrpcLoadBalancer customBalancer = new JQuickGrpcLoadBalancer() {
            @Override
            public JQuickGrpcServiceInstance select(
                    List<JQuickGrpcServiceInstance> instances) {
                return instances.get(0);
            }

            @Override
            public String getName() {
                return "Custom";
            }
        };

        factory.registerLoadBalancer("custom", customBalancer);
        JQuickGrpcLoadBalancer retrieved = factory.switchLoadBalancer("custom");
        assertEquals("Custom", retrieved.getName());
    }

    @Test
    void testGetCurrentStats() {
        JQuickGrpcClientConfig clientConfig = new JQuickGrpcClientConfig();
        factory.createClient(clientConfig, null, null);
        Map<String, Object> stats = factory.getCurrentStats();
        assertNotNull(stats);
        assertTrue(stats.containsKey("activeServerRunning"));
        assertTrue(stats.containsKey("activeClientClosed"));
        assertTrue(stats.containsKey("clientStats"));
    }
}