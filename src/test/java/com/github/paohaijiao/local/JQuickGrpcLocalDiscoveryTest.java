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
package com.github.paohaijiao.local;

import com.github.paohaijiao.console.JConsole;
import com.github.paohaijiao.grpc.discovery.impl.JQuickGrpcLocalDiscovery;
import com.github.paohaijiao.grpc.domain.JQuickGrpcServiceInstance;
import com.github.paohaijiao.grpc.metadata.JQuickServiceInstanceMetrics;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class JQuickGrpcLocalDiscoveryTest {

    private static final JConsole log = JConsole.getInstance();
    private static JQuickGrpcLocalDiscovery discovery;

    @BeforeAll
    static void setUpAll() {
        log.info("========== Starting Local Discovery Tests ==========");
        discovery = new JQuickGrpcLocalDiscovery();
        log.info("Local discovery created");
    }

    @AfterAll
    static void tearDownAll() {
        log.info("========== Cleaning up tests ==========");
        if (discovery != null) {
            discovery.unregisterAllServices();
            discovery.close();
        }
        log.info("Local discovery closed");
    }

    @BeforeEach
    void setUp() {
        discovery.clear();
    }

    @Test
    void testRegisterService() {
        log.info("=== 测试服务注册 ===");
        discovery.registerService("test-service", "192.168.1.100", 9090, 5);
        List<JQuickGrpcServiceInstance> instances = discovery.getInstances("test-service");
        assertNotNull(instances);
        assertFalse(instances.isEmpty());
        assertEquals("test-service", instances.get(0).getServiceName());
        assertEquals("192.168.1.100", instances.get(0).getHost());
        assertEquals(9090, instances.get(0).getPort());
        assertEquals(5, instances.get(0).getWeight());
        assertTrue(instances.get(0).isHealthy());
        log.info(" 服务注册成功: {}", instances.get(0).getAddress());
    }

    @Test
    void testRegisterMultipleInstances() {
        log.info("=== 测试注册多个实例 ===");
        String serviceName = "test-multi-service";
        discovery.registerService(serviceName, "192.168.1.101", 9091, 1);
        discovery.registerService(serviceName, "192.168.1.102", 9092, 2);
        discovery.registerService(serviceName, "192.168.1.103", 9093, 3);
        List<JQuickGrpcServiceInstance> instances = discovery.getInstances(serviceName);
        assertEquals(3, instances.size());
        int totalWeight = instances.stream().mapToInt(JQuickGrpcServiceInstance::getWeight).sum();
        assertEquals(6, totalWeight);
        for (JQuickGrpcServiceInstance instance : instances) {
            log.info(" 实例: {} weight={}", instance.getAddress(), instance.getWeight());
        }
        log.info(" 成功注册 {} 个实例", instances.size());
    }

    @Test
    void testRegisterDifferentServices() {
        log.info("=== 测试注册多个不同的服务 ===");
        discovery.registerService("service-a", "192.168.1.101", 9091, 1);
        discovery.registerService("service-b", "192.168.1.102", 9092, 2);
        discovery.registerService("service-c", "192.168.1.103", 9093, 3);
        assertEquals(1, discovery.getInstances("service-a").size());
        assertEquals(1, discovery.getInstances("service-b").size());
        assertEquals(1, discovery.getInstances("service-c").size());
        Set<String> serviceNames = discovery.getAllServiceNames();
        assertEquals(3, serviceNames.size());
        log.info(" 成功注册 {} 个不同的服务", serviceNames.size());
    }

    @Test
    void testRegisterWithMetrics() {
        log.info("=== 测试带 Metrics 的服务注册 ===");
        JQuickServiceInstanceMetrics metrics = new JQuickServiceInstanceMetrics();
        metrics.setCpuUsage(45.5);
        metrics.setMemoryUsage(60.2);
        metrics.setActiveRequests(10);
        discovery.registerService("test-metrics-service", "192.168.1.100", 9090, 5, metrics);
        List<JQuickGrpcServiceInstance> instances = discovery.getInstances("test-metrics-service");
        assertNotNull(instances.get(0).getMetrics());
        assertEquals(45.5, instances.get(0).getMetrics().getCpuUsage(), 0.01);
        assertEquals(60.2, instances.get(0).getMetrics().getMemoryUsage(), 0.01);
        assertEquals(10, instances.get(0).getMetrics().getActiveRequests());
        log.info(" Metrics 注册成功: CPU={}%", instances.get(0).getMetrics().getCpuUsage());
    }

    @Test
    void testGetInstances() {
        log.info("=== 测试服务发现 ===");
        discovery.registerService("test-discovery-service", "10.0.0.1", 8080, 10);
        List<JQuickGrpcServiceInstance> instances = discovery.getInstances("test-discovery-service");
        assertEquals(1, instances.size());
        assertEquals("10.0.0.1", instances.get(0).getHost());
        assertEquals(8080, instances.get(0).getPort());
        log.info(" 服务发现成功: {}", instances.get(0).getAddress());
    }

    @Test
    void testServiceChangeListener() throws InterruptedException {
        log.info("=== 测试服务变更监听 ===");
        String serviceName = "test-watch-service";
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<List<JQuickGrpcServiceInstance>> receivedInstances = new AtomicReference<>();
        discovery.subscribe(serviceName, (name, instances) -> {
            receivedInstances.set(instances);
            log.info(" 收到变更通知: {} 个实例", instances.size());
            latch.countDown();
        });
        discovery.registerService(serviceName, "192.168.1.10", 8010, 1);
        boolean received = latch.await(5, TimeUnit.SECONDS);
        assertTrue(received);
        assertNotNull(receivedInstances.get());
        assertEquals(1, receivedInstances.get().size());
        log.info(" 收到服务变更通知");
    }

    @Test
    void testUpdateHealth() {
        log.info("=== 测试健康状态更新 ===");
        String serviceName = "test-health-service";
        String host = "192.168.1.50";
        int port = 7070;
        discovery.registerService(serviceName, host, port, 1);
        List<JQuickGrpcServiceInstance> instances = discovery.getInstances(serviceName);
        assertTrue(instances.get(0).isHealthy());
        log.info(" 初始状态: 健康");
        discovery.updateHealth(serviceName, host, port, false);
        instances = discovery.getInstances(serviceName);
        assertTrue(instances.isEmpty(), "不健康的实例不应该被返回");
        log.info(" 健康状态: 不健康（已被过滤）");
        List<JQuickGrpcServiceInstance> allInstances = discovery.getAllInstances(serviceName);
        assertFalse(allInstances.get(0).isHealthy());
        log.info(" 健康状态: 不健康（原始数据）");
        discovery.updateHealth(serviceName, host, port, true);
        instances = discovery.getInstances(serviceName);
        assertTrue(instances.get(0).isHealthy());
        log.info(" 健康状态: 健康");
        log.info(" 健康状态变化测试完成");
    }

    @Test
    void testUpdateWeight() {
        log.info("=== 测试权重更新 ===");
        String serviceName = "test-weight-service";
        String host = "192.168.1.60";
        int port = 6060;
        discovery.registerService(serviceName, host, port, 1);
        List<JQuickGrpcServiceInstance> instances = discovery.getInstances(serviceName);
        assertEquals(1, instances.get(0).getWeight());
        log.info(" 初始权重: 1");
        discovery.updateWeight(serviceName, host, port, 10);
        instances = discovery.getInstances(serviceName);
        assertEquals(10, instances.get(0).getWeight());
        log.info(" 更新后权重: 10");
        log.info(" 权重更新测试完成");
    }

    @Test
    void testUnregisterService() {
        log.info("=== 测试服务注销 ===");
        String serviceName = "test-unregister-service";
        String host = "192.168.1.200";
        int port = 9500;
        discovery.registerService(serviceName, host, port, 1);
        assertEquals(1, discovery.getInstances(serviceName).size());
        log.info(" 注册成功");
        discovery.unregisterService(serviceName, host, port);
        List<JQuickGrpcServiceInstance> after = discovery.getInstances(serviceName);
        assertTrue(after.isEmpty());
        log.info(" 服务注销成功");
    }

    @Test
    void testUnregisterAllServices() {
        log.info("=== 测试注销所有服务 ===");
        discovery.registerService("service-1", "192.168.1.1", 8001, 1);
        discovery.registerService("service-2", "192.168.1.2", 8002, 2);
        discovery.registerService("service-3", "192.168.1.3", 8003, 3);
        assertEquals(3, discovery.getAllServiceNames().size());
        log.info(" 注册了 3 个服务");
        discovery.unregisterAllServices();
        assertEquals(0, discovery.getAllServiceNames().size());
        log.info(" 所有服务都已注销");
    }

    @Test
    void testGetStats() {
        log.info("=== 测试获取统计信息 ===");
        discovery.registerService("stats-service-1", "192.168.1.1", 8001, 1);
        discovery.registerService("stats-service-2", "192.168.1.2", 8002, 2);
        Map<String, Object> stats = discovery.getStats();
        assertEquals(2, stats.get("serviceCount"));
        assertEquals(2, stats.get("instanceCount"));
        log.info("✓ 统计信息: serviceCount={}, instanceCount={}", stats.get("serviceCount"), stats.get("instanceCount"));
    }

    @Test
    void testDuplicateRegistration() {
        log.info("=== 测试重复注册相同实例 ===");
        String serviceName = "test-duplicate-service";
        String host = "192.168.1.1";
        int port = 10001;
        discovery.registerService(serviceName, host, port, 1);
        discovery.registerService(serviceName, host, port, 2);
        List<JQuickGrpcServiceInstance> instances = discovery.getInstances(serviceName);
        // 应该只有1个实例，权重应该是第一次注册的值
        assertEquals(1, instances.size());
        assertEquals(1, instances.get(0).getWeight());
        log.info(" 重复注册被正确忽略");
    }

    @Test
    void testGetNonExistentService() {
        log.info("=== 测试获取不存在的服务 ===");
        List<JQuickGrpcServiceInstance> instances = discovery.getInstances("non-existent-service");
        assertNotNull(instances);
        assertTrue(instances.isEmpty());
        log.info("✓ 获取不存在服务返回空列表");
    }
}
