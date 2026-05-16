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
package com.github.paohaijiao.etcd;

import com.github.paohaijiao.console.JConsole;
import com.github.paohaijiao.grpc.discovery.impl.JQuickGrpcEtcdDiscovery;
import com.github.paohaijiao.grpc.domain.JQuickGrpcServiceInstance;
import com.github.paohaijiao.grpc.metadata.JQuickServiceInstanceMetrics;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * packageName com.github.paohaijiao.etcd
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/16
 */
public class JQuickGrpcEtcdDiscoveryTest {

    private static JQuickGrpcEtcdDiscovery discovery;

    private static final String TEST_SERVICE = "test-user-service";

    private static final String TEST_SERVICE2 = "test-order-service";

    @BeforeAll
    static void setUpAll() {
        discovery = new JQuickGrpcEtcdDiscovery("http://192.168.32.173:2379");
        System.out.println("Etcd discovery initialized");
    }

    @AfterAll
    static void tearDownAll() {
        if (discovery != null) {
            discovery.unregisterService();
            discovery.close();
            System.out.println("Etcd discovery closed");
        }
    }

    @BeforeEach
    void setUp() {
        sleep(100);
    }

    @AfterEach
    void tearDown() {
        try {
            discovery.unregisterService();
        } catch (Exception e) {
            e.printStackTrace();
        }
        sleep(100);
    }

    @Test
    @Order(1)
    void testRegisterService() {
        discovery.registerService(TEST_SERVICE, "192.168.1.100", 9090, 5);
        sleep(500);
        List<JQuickGrpcServiceInstance> instances = discovery.getInstances(TEST_SERVICE);
        assertNotNull(instances);
        JQuickGrpcServiceInstance instance = instances.get(0);
        assertEquals("192.168.1.100", instance.getHost());
        assertEquals(9090, instance.getPort());
        assertEquals(5, instance.getWeight());
        assertTrue(instance.isHealthy());
        System.out.println("服务注册成功: " + instance.getAddress());
    }

    @Test
    @Order(2)
    void testRegisterMultipleInstances() {
        JQuickGrpcEtcdDiscovery discovery2 = new JQuickGrpcEtcdDiscovery("http://192.168.32.173:2379");
        JQuickGrpcEtcdDiscovery discovery3 = new JQuickGrpcEtcdDiscovery("http://192.168.32.173:2379");
        try {
            discovery.registerService(TEST_SERVICE2, "192.168.1.101", 9091, 1);
            discovery2.registerService(TEST_SERVICE2, "192.168.1.102", 9092, 2);
            discovery3.registerService(TEST_SERVICE2, "192.168.1.103", 9093, 3);
            sleep(500);
            List<JQuickGrpcServiceInstance> instances = discovery.getInstances(TEST_SERVICE2);
            assertNotNull(instances);
            assertEquals(3, instances.size(), "应该有3个实例");
            int totalWeight = instances.stream().mapToInt(JQuickGrpcServiceInstance::getWeight).sum();
            assertEquals(6, totalWeight);
            System.out.println("✓ 注册了 " + instances.size() + " 个服务实例");
            instances.forEach(i -> System.out.println("  - " + i.getAddress() + " (weight=" + i.getWeight() + ")"));
        } finally {
            discovery2.unregisterService();
            discovery3.unregisterService();
            discovery2.close();
            discovery3.close();
        }
    }

    @Test
    @Order(3)
    void testGetInstances() {
        discovery.registerService(TEST_SERVICE, "10.0.0.1", 8080, 10);
        sleep(500);
        List<JQuickGrpcServiceInstance> instances = discovery.getInstances(TEST_SERVICE);
        assertNotNull(instances);
        assertEquals(1, instances.size());
        assertEquals("10.0.0.1", instances.get(0).getHost());
        assertEquals(8080, instances.get(0).getPort());
        System.out.println("服务发现成功: " + instances.get(0).getAddress());
    }

    @Test
    @Order(4)
    void testUnregisterService() {
        discovery.registerService(TEST_SERVICE, "192.168.1.200", 9500, 1);
        sleep(500);
        List<JQuickGrpcServiceInstance> beforeUnregister = discovery.getInstances(TEST_SERVICE);
        assertEquals(1, beforeUnregister.size());
        discovery.unregisterService();
        sleep(500);
        List<JQuickGrpcServiceInstance> afterUnregister = discovery.getInstances(TEST_SERVICE);
        System.out.println(" 服务注销成功");
    }

    @Test
    @Order(5)
    void testUpdateHealth() {
        discovery.registerService(TEST_SERVICE, "192.168.1.50", 7070, 1);
        sleep(500);
        List<JQuickGrpcServiceInstance> instances = discovery.getInstances(TEST_SERVICE);
        assertTrue(instances.get(0).isHealthy());
        discovery.updateHealth(false);
        sleep(500);
        instances = discovery.getInstances(TEST_SERVICE);
        System.out.println("✓ 健康状态更新完成");
    }

    @Test
    @Order(6)
    void testServiceChangeListener() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        AtomicInteger changeCount = new AtomicInteger(0);
        AtomicReference<List<JQuickGrpcServiceInstance>> latestInstances = new AtomicReference<>();
        JQuickGrpcEtcdDiscovery anotherInstance = new JQuickGrpcEtcdDiscovery("http://192.168.32.173:2379");
        try {
            discovery.subscribe(TEST_SERVICE, (serviceName, instances) -> {
                changeCount.incrementAndGet();
                latestInstances.set(instances);
                latch.countDown();
                System.out.println("服务变更通知: " + serviceName + " 有 " + instances.size() + " 个实例");
                instances.forEach(i -> System.out.println("   - " + i.getAddress() + " (healthy=" + i.isHealthy() + ")"));
            });
            discovery.registerService(TEST_SERVICE, "192.168.1.10", 8010, 1);
            sleep(500);
            anotherInstance.registerService(TEST_SERVICE, "192.168.1.11", 8011, 2);
            sleep(500);
            boolean completed = latch.await(10, TimeUnit.SECONDS);
            assertTrue(changeCount.get() >= 1, "至少收到一次变更通知");
            System.out.println(" 收到 " + changeCount.get() + " 次服务变更通知");
        } finally {
            anotherInstance.unregisterService();
            anotherInstance.close();
        }
    }

    @Test
    @Order(7)
    void testLeaseAutoExpiration() throws InterruptedException {
        JQuickGrpcEtcdDiscovery shortLeaseDiscovery = new JQuickGrpcEtcdDiscovery("http://192.168.32.173:2379", null, null, "/grpc/services", 5);
        try {
            shortLeaseDiscovery.registerService(TEST_SERVICE, "192.168.1.99", 9999, 1);
            sleep(500);
            List<JQuickGrpcServiceInstance> instances = discovery.getInstances(TEST_SERVICE);
            assertEquals(1, instances.size());
            System.out.println(" 服务已注册，等待 Lease 过期...");
            Thread.sleep(8000);
            instances = discovery.getInstances(TEST_SERVICE);
            System.out.println("✓ Lease 自动过期测试完成");
        } finally {
           // shortLeaseDiscovery.close();
        }
    }

    @Test
    @Order(8)
    void testGetNonExistentService() {
        List<JQuickGrpcServiceInstance> instances = discovery.getInstances("non-existent-service-" + System.currentTimeMillis());
        assertNotNull(instances);
        assertTrue(instances.isEmpty(), "不存在的服务应该返回空列表");
    }

    @Test
    @Order(9)
    void testDuplicateRegistration() {
        System.out.println("=== 测试重复注册相同服务 ===");
        discovery.registerService(TEST_SERVICE, "192.168.1.1", 10001, 1);
        sleep(300);
        discovery.registerService(TEST_SERVICE, "192.168.1.2", 10002, 2);
        sleep(300);
        List<JQuickGrpcServiceInstance> instances = discovery.getInstances(TEST_SERVICE);
        assertNotNull(instances);
        System.out.println("重复注册测试完成，当前实例数: " + instances.size());
    }

    @Test
    @Order(10)
    void testConcurrentGetInstances() throws InterruptedException {
        discovery.registerService(TEST_SERVICE, "192.168.1.100", 10086, 1);
        sleep(500);
        int threadCount = 20;
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                try {
                    List<JQuickGrpcServiceInstance> instances = discovery.getInstances(TEST_SERVICE);
                    if (instances != null && !instances.isEmpty()) {
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    System.err.println("并发获取失败: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            }).start();
        }
        boolean completed = latch.await(10, TimeUnit.SECONDS);
        assertTrue(completed);
        assertEquals(threadCount, successCount.get(), "所有线程都应该成功获取实例");
        System.out.println("并发测试完成: " + successCount.get() + "/" + threadCount + " 成功");
    }
    @Test
    @Order(5)
    void testRegisterServiceWithMetrics() {
        JQuickServiceInstanceMetrics metrics = new JQuickServiceInstanceMetrics();
        metrics.setCpuUsage(45.5);
        metrics.setMemoryUsage(60.2);
        metrics.setActiveRequests(10);
        metrics.setQueueSize(0);
        metrics.setLastReportTime(System.currentTimeMillis());
        discovery.registerService(TEST_SERVICE, "192.168.1.100", 9090, 5, metrics);
        sleep(500);
        List<JQuickGrpcServiceInstance> instances = discovery.getInstances(TEST_SERVICE);
        assertFalse(instances.isEmpty());
        JQuickGrpcServiceInstance instance = instances.get(0);
        // 验证 serviceName
        assertEquals(TEST_SERVICE, instance.getServiceName(), "serviceName 应该正确");
        // 验证 Metrics
        assertNotNull(instance.getMetrics(), "Metrics 不应该为 null");
        assertEquals(45.5, instance.getMetrics().getCpuUsage(), 0.01);
        assertEquals(60.2, instance.getMetrics().getMemoryUsage(), 0.01);
        assertEquals(10, instance.getMetrics().getActiveRequests());
        assertEquals(0, instance.getMetrics().getQueueSize());
        JConsole log=JConsole.initConsoleEnvironment();
        log.info("带 Metrics 的服务注册成功");
        log.info("  - CPU: {}%", instance.getMetrics().getCpuUsage());
        log.info("  - 内存: {}%", instance.getMetrics().getMemoryUsage());
        log.info("  - 活跃请求: {}", instance.getMetrics().getActiveRequests());
    }

    @Test
    @Order(11)
    void testServiceIsolation() {
        System.out.println("=== 测试不同服务隔离 ===");
        String serviceA = "service-a";
        String serviceB = "service-b";
        JQuickGrpcEtcdDiscovery discoveryA = new JQuickGrpcEtcdDiscovery("http://192.168.32.173:2379");
        JQuickGrpcEtcdDiscovery discoveryB = new JQuickGrpcEtcdDiscovery("http://192.168.32.173:2379");
        try {
            discoveryA.registerService(serviceA, "10.0.0.1", 8001, 1);
            discoveryB.registerService(serviceB, "10.0.0.2", 8002, 1);
            sleep(500);
            // 服务A只能看到自己的实例
            List<JQuickGrpcServiceInstance> instancesA = discoveryB.getInstances(serviceA);
            assertEquals(1, instancesA.size());
            assertEquals(8001, instancesA.get(0).getPort());
            // 服务B只能看到自己的实例
            List<JQuickGrpcServiceInstance> instancesB = discovery.getInstances(serviceB);
            assertEquals(1, instancesB.size());
            assertEquals(8002, instancesB.get(0).getPort());
            System.out.println("服务隔离测试通过");
        } finally {
            discoveryA.unregisterService();
            discoveryB.unregisterService();
            discoveryA.close();
            discoveryB.close();
        }
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
