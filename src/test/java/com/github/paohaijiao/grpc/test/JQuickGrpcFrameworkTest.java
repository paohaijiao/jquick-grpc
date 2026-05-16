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
 *
 * Copyright (c) [2025-2099] Martin (goudingcheng@gmail.com)
 */
package com.github.paohaijiao.grpc.test;

/**
 * packageName com.github.paohaijiao.grpc.test
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/16
 */
import com.github.paohaijiao.grpc.client.JQuickGrpcClient;
import com.github.paohaijiao.grpc.client.impl.JQuickGrpcPooledClient;
import com.github.paohaijiao.grpc.client.impl.JQuickGrpcSingleClient;
import com.github.paohaijiao.grpc.config.JQuickGrpcClientConfig;
import com.github.paohaijiao.grpc.discovery.impl.JQuickGrpcLocalDiscovery;
import com.github.paohaijiao.grpc.domain.JQuickGrpcServiceInstance;
import com.github.paohaijiao.grpc.loadbalance.impl.JQuickGrpcRoundRobinLoadBalancer;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JQuickGrpcFrameworkTest {

    private JQuickGrpcLocalDiscovery discovery;

    @BeforeEach
    void setUp() {
        discovery = new JQuickGrpcLocalDiscovery();
        // 模拟注册服务
        discovery.registerService("TestService", "localhost", 9090);
        discovery.registerService("TestService", "localhost", 9091);
    }

    @AfterEach
    void tearDown() {
        discovery.close();
    }

    @Test
    void testLocalDiscovery() {
        List<JQuickGrpcServiceInstance> instances = discovery.getInstances("TestService");
        assertEquals(2, instances.size());
        assertTrue(instances.get(0).isHealthy());
        assertEquals(1, instances.get(0).getWeight());
    }

    @Test
    void testPooledClientCreation() {
        JQuickGrpcClientConfig config = new JQuickGrpcClientConfig();
        config.setClientType("pooled");
        JQuickGrpcClient client = new JQuickGrpcPooledClient(
                config,
                discovery,
                new JQuickGrpcRoundRobinLoadBalancer()
        );
        assertNotNull(client);
        assertEquals("pooled", client.getClientType());
        assertFalse(client.isClosed());
        client.close();
        assertTrue(client.isClosed());
    }

    @Test
    void testSingleClientCreation() {
        JQuickGrpcClientConfig config = new JQuickGrpcClientConfig();
        config.setClientType("single");
        JQuickGrpcClient client = new JQuickGrpcSingleClient(
                config,
                discovery,
                new JQuickGrpcRoundRobinLoadBalancer()
        );

        assertEquals("single", client.getClientType());
        client.close();
    }

    @Test
    void testConfigDefaults() {
        JQuickGrpcClientConfig config = new JQuickGrpcClientConfig();
        assertEquals("pooled", config.getClientType());
        assertEquals(3, config.getMaxRetries());
        assertEquals(5000, config.getDeadlineMillis());
        assertTrue(config.isUsePlaintext());
    }

    @Test
    void testPooledConfigBuilder() {
        JQuickGrpcClientConfig config = JQuickGrpcClientConfig.pooled();
        assertEquals("pooled", config.getClientType());
    }

    @Test
    void testDiscoveryStats() {
        Map<String, Object> stats = discovery.getStats();
        assertEquals(1, stats.get("serviceCount"));
        assertEquals(2, stats.get("instanceCount"));
        assertFalse((Boolean) stats.get("closed"));
    }
}