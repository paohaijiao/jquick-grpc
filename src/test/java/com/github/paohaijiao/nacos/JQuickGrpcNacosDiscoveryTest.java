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
package com.github.paohaijiao.nacos;

import com.github.paohaijiao.console.JConsole;
import com.github.paohaijiao.grpc.discovery.impl.JQuickGrpcNacosDiscovery;
import com.github.paohaijiao.grpc.domain.JQuickGrpcServiceInstance;
import com.github.paohaijiao.grpc.metadata.JQuickServiceInstanceMetrics;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class JQuickGrpcNacosDiscoveryTest {

    private static final JConsole log = JConsole.getInstance();
    private static final String NACOS_SERVER = "192.168.32.173:8848";
    private static final String NACOS_USERNAME = "nacos";
    private static final String NACOS_PASSWORD = "nacos";
    private static JQuickGrpcNacosDiscovery discovery;

    @BeforeAll
    static void setUpAll() {
        log.info("========== Starting Nacos Discovery Tests ==========");
        discovery = new JQuickGrpcNacosDiscovery(NACOS_SERVER, NACOS_USERNAME, NACOS_PASSWORD);
        log.info("Nacos discovery created");
    }

    @AfterAll
    static void tearDownAll() {
        log.info("========== Cleaning up tests ==========");
        if (discovery != null) {
            discovery.unregisterAllServices();
            discovery.close();
        }
        log.info("Nacos discovery closed");
    }

    @BeforeEach
    void setUp() {
        sleep(500);
    }

    @AfterEach
    void tearDown() {
        try {
            discovery.unregisterAllServices();
        } catch (Exception e) {
            log.warn("Cleanup error: {}", e.getMessage());
        }
        sleep(500);
    }

    @Test
    @Order(1)
    @DisplayName("1. 测试服务注册")
    void testRegisterService() throws InterruptedException {
        String serviceName = "test-register-service";
        String host = "192.168.1.100";
        int port = 9090;
        int weight = 5;
        discovery.registerService(serviceName, host, port, weight);
        Thread.sleep(3000);
        List<JQuickGrpcServiceInstance> instances = discovery.getInstances(serviceName);
        assertNotNull(instances);
        assertFalse(instances.isEmpty());
        assertEquals(serviceName, instances.get(0).getServiceName());
        assertEquals(host, instances.get(0).getHost());
        assertEquals(port, instances.get(0).getPort());
        assertEquals(weight, instances.get(0).getWeight());
        assertTrue(instances.get(0).isHealthy());
        log.info("服务注册成功: {} -> {}", serviceName, instances.get(0).getAddress());
    }

    @Test
    @Order(2)
    @DisplayName("2. 测试注册多个不同的服务")
    void testRegisterMultipleDifferentServices() throws InterruptedException {
        log.info("=== 测试注册多个不同的服务 ===");
        discovery.registerService("service-a", "192.168.1.101", 9091, 1);
        discovery.registerService("service-b", "192.168.1.102", 9092, 2);
        discovery.registerService("service-c", "192.168.1.103", 9093, 3);
        Thread.sleep(3000);
        assertEquals(1, discovery.getInstances("service-a").size());
        assertEquals(1, discovery.getInstances("service-b").size());
        assertEquals(1, discovery.getInstances("service-c").size());
        log.info("成功注册 {} 个不同的服务", 3);
        discovery.unregisterService("service-a", "192.168.1.101", 9091);
        discovery.unregisterService("service-b", "192.168.1.102", 9092);
        discovery.unregisterService("service-c", "192.168.1.103", 9093);
        Thread.sleep(2000);
        log.info("多个服务注册测试完成");
    }

    @Test
    @Order(3)
    @DisplayName("3. 测试注册多个实例（同一个服务）")
    void testRegisterMultipleInstancesSameService() throws InterruptedException {
        log.info("=== 测试注册多个实例（同一个服务） ===");
        String serviceName = "test-multi-instance-service";
        discovery.registerService(serviceName, "192.168.1.101", 9091, 1);
        discovery.registerService(serviceName, "192.168.1.102", 9092, 2);
        discovery.registerService(serviceName, "192.168.1.103", 9093, 3);
        Thread.sleep(3000);
        List<JQuickGrpcServiceInstance> instances = discovery.getInstances(serviceName);
        assertEquals(3, instances.size());
        int totalWeight = instances.stream().mapToInt(JQuickGrpcServiceInstance::getWeight).sum();
        assertEquals(6, totalWeight);
        for (JQuickGrpcServiceInstance instance : instances) {
            log.info(" 实例: {} weight={}", instance.getAddress(), instance.getWeight());
        }

        log.info("成功注册 {} 个实例", instances.size());
    }

    @Test
    @Order(4)
    @DisplayName("4. 测试带 Metrics 的服务注册")
    void testRegisterServiceWithMetrics() throws InterruptedException {
        log.info("=== 测试带 Metrics 的服务注册 ===");
        String serviceName = "test-metrics-service";
        JQuickServiceInstanceMetrics metrics = new JQuickServiceInstanceMetrics();
        metrics.setCpuUsage(45.5);
        metrics.setMemoryUsage(60.2);
        metrics.setActiveRequests(10);
        metrics.setQueueSize(0);
        metrics.setLastReportTime(System.currentTimeMillis());
        discovery.registerService(serviceName, "192.168.1.100", 9090, 5, metrics);
        Thread.sleep(3000);
        List<JQuickGrpcServiceInstance> instances = discovery.getInstances(serviceName);
        assertFalse(instances.isEmpty());
        JQuickGrpcServiceInstance instance = instances.get(0);
        assertEquals(serviceName, instance.getServiceName());
        assertNotNull(instance.getMetrics());
        assertEquals(45.5, instance.getMetrics().getCpuUsage(), 0.01);
        assertEquals(60.2, instance.getMetrics().getMemoryUsage(), 0.01);
        assertEquals(10, instance.getMetrics().getActiveRequests());
        log.info(" 带 Metrics 的服务注册成功");
        log.info("  - CPU: {}%", instance.getMetrics().getCpuUsage());
        log.info("  - 内存: {}%", instance.getMetrics().getMemoryUsage());
        log.info("  - 活跃请求: {}", instance.getMetrics().getActiveRequests());
    }

    @Test
    @Order(5)
    @DisplayName("5. 测试服务发现")
    void testGetInstances() throws InterruptedException {
        log.info("=== 测试服务发现 ===");

        String serviceName = "test-discovery-service";
        String host = "10.0.0.1";
        int port = 8080;
        int weight = 10;
        discovery.registerService(serviceName, host, port, weight);
        Thread.sleep(3000);
        List<JQuickGrpcServiceInstance> instances = discovery.getInstances(serviceName);
        assertNotNull(instances);
        assertEquals(1, instances.size());
        assertEquals(serviceName, instances.get(0).getServiceName());
        assertEquals(host, instances.get(0).getHost());
        assertEquals(port, instances.get(0).getPort());
        assertEquals(weight, instances.get(0).getWeight());

        log.info("✓ 服务发现成功: {}", instances.get(0).getAddress());
    }

    @Test
    @Order(6)
    @DisplayName("6. 测试服务变更监听")
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
        // 注册服务触发变更
        discovery.registerService(serviceName, "192.168.1.10", 8010, 1);
        boolean received = latch.await(15, TimeUnit.SECONDS);
        assertTrue(received, "应该收到服务变更通知");
        assertNotNull(receivedInstances.get());
        log.info("收到服务变更通知");
    }

    @Test
    @Order(7)
    @DisplayName("7. 测试服务注销后变更通知")
    void testUnregisterChangeListener() throws InterruptedException {
        log.info("=== 测试服务注销后变更通知 ===");
        String serviceName = "test-unregister-watch-service";
        String host = "192.168.1.200";
        int port = 9500;
        CountDownLatch registerLatch = new CountDownLatch(1);
        CountDownLatch unregisterLatch = new CountDownLatch(1);
        discovery.subscribe(serviceName, (name, instances) -> {
            log.info(" 服务变更: {} 个实例", instances.size());
            if (instances.isEmpty()) {
                unregisterLatch.countDown();
            } else {
                registerLatch.countDown();
            }
        });
        // 注册服务
        discovery.registerService(serviceName, host, port, 1);
        boolean registerReceived = registerLatch.await(15, TimeUnit.SECONDS);
        assertTrue(registerReceived, "应该收到注册通知");
        log.info(" 收到注册通知");
        // 注销服务
        discovery.unregisterService(serviceName, host, port);
        boolean unregisterReceived = unregisterLatch.await(15, TimeUnit.SECONDS);
        assertTrue(unregisterReceived, "应该收到注销通知");
        log.info(" 收到注销通知");
    }

    @Test
    @Order(8)
    @DisplayName("8. 测试健康状态更新")
    void testUpdateHealth() throws InterruptedException {
        log.info("=== 测试健康状态更新 ===");
        String serviceName = "test-health-service";
        String host = "192.168.1.50";
        int port = 7070;
        discovery.registerService(serviceName, host, port, 1);
        Thread.sleep(3000);
        // 验证初始健康状态
        List<JQuickGrpcServiceInstance> instances = discovery.getInstances(serviceName);
        assertTrue(instances.get(0).isHealthy(), "初始状态应该健康");
        log.info("初始状态: 健康");
        // 更新为不健康
        discovery.updateHealth(serviceName, host, port, false);
        log.info(" 已设置不健康状态");
        Thread.sleep(3000);

        // 验证不健康状态
        instances = discovery.getInstances(serviceName);
        if (!instances.isEmpty()) {
            assertFalse(instances.get(0).isHealthy(), "应该变为不健康");
            log.info("健康状态: 不健康");
        }

        // 恢复健康
        discovery.updateHealth(serviceName, host, port, true);
        log.info(" 已恢复健康状态");
        Thread.sleep(3000);

        // 验证恢复健康
        instances = discovery.getInstances(serviceName);
        if (!instances.isEmpty()) {
            assertTrue(instances.get(0).isHealthy(), "应该恢复健康");
            log.info(" 健康状态: 健康");
        }

        log.info(" 健康状态变化测试完成");
    }
    @Test
    @Order(9)
    @DisplayName("9. 测试更新 Metrics")
    void testUpdateMetrics() throws InterruptedException {
        log.info("=== 测试更新 Metrics ===");
        String serviceName = "test-update-metrics-service";
        String host = "192.168.1.101";
        int port = 9091;

        // 初始 Metrics
        JQuickServiceInstanceMetrics initialMetrics = new JQuickServiceInstanceMetrics();
        initialMetrics.setCpuUsage(10.0);
        initialMetrics.setMemoryUsage(20.0);
        initialMetrics.setActiveRequests(5);
        initialMetrics.setLastReportTime(System.currentTimeMillis());

        discovery.registerService(serviceName, host, port, 1, initialMetrics);
        Thread.sleep(3000);

        // 验证初始 Metrics
        List<JQuickGrpcServiceInstance> instances = discovery.getInstances(serviceName);
        assertEquals(10.0, instances.get(0).getMetrics().getCpuUsage(), 0.01);
        log.info(" 初始 Metrics: CPU={}%", instances.get(0).getMetrics().getCpuUsage());

        // 更新 Metrics
        JQuickServiceInstanceMetrics updatedMetrics = new JQuickServiceInstanceMetrics();
        updatedMetrics.setCpuUsage(85.5);
        updatedMetrics.setMemoryUsage(90.2);
        updatedMetrics.setActiveRequests(30);
        updatedMetrics.setQueueSize(5);
        updatedMetrics.setLastReportTime(System.currentTimeMillis());

        discovery.updateMetrics(serviceName, host, port, updatedMetrics);
        Thread.sleep(3000);

        // 验证更新后的 Metrics
        instances = discovery.getInstances(serviceName);
        assertEquals(85.5, instances.get(0).getMetrics().getCpuUsage(), 0.01);
        assertEquals(90.2, instances.get(0).getMetrics().getMemoryUsage(), 0.01);
        assertEquals(30, instances.get(0).getMetrics().getActiveRequests());

        log.info(" 更新后 Metrics: CPU={}%, 内存={}%, 活跃请求={}",
                instances.get(0).getMetrics().getCpuUsage(),
                instances.get(0).getMetrics().getMemoryUsage(),
                instances.get(0).getMetrics().getActiveRequests());
    }


    @Test
    @Order(10)
    @DisplayName("10. 测试服务注销")
    void testUnregisterService() throws InterruptedException {
        log.info("=== 测试服务注销 ===");
        String serviceName = "test-unregister-service";
        String host = "192.168.1.200";
        int port = 9500;
        discovery.registerService(serviceName, host, port, 1);
        Thread.sleep(3000);
        List<JQuickGrpcServiceInstance> before = discovery.getInstances(serviceName);
        assertEquals(1, before.size());
        assertTrue(before.get(0).isHealthy());
        log.info(" 注册成功，实例数: {}", before.size());
        discovery.unregisterService(serviceName, host, port);
        log.info(" 已执行注销");
        Thread.sleep(5000);
        List<JQuickGrpcServiceInstance> after = discovery.getInstances(serviceName);
        boolean instanceRemoved = after.isEmpty() || after.stream().noneMatch(i -> i.getPort() == port && i.isHealthy());
        assertTrue(instanceRemoved, "实例应该被注销");
        log.info(" 注销后实例数: {}", after.size());
        log.info(" 服务注销测试完成");
    }

    @Test
    @Order(11)
    @DisplayName("11. 测试注销所有服务")
    void testUnregisterAllServices() throws InterruptedException {
        log.info("=== 测试注销所有服务 ===");

        discovery.registerService("service-1", "192.168.1.1", 8001, 1);
        discovery.registerService("service-2", "192.168.1.2", 8002, 2);
        discovery.registerService("service-3", "192.168.1.3", 8003, 3);
        Thread.sleep(3000);
        assertEquals(1, discovery.getInstances("service-1").size());
        assertEquals(1, discovery.getInstances("service-2").size());
        assertEquals(1, discovery.getInstances("service-3").size());
        log.info(" 注册了 3 个服务");
        discovery.unregisterAllServices();
        Thread.sleep(5000);
        assertTrue(discovery.getInstances("service-1").isEmpty() || discovery.getInstances("service-1").stream().noneMatch(JQuickGrpcServiceInstance::isHealthy));
        assertTrue(discovery.getInstances("service-2").isEmpty() || discovery.getInstances("service-2").stream().noneMatch(JQuickGrpcServiceInstance::isHealthy));
        assertTrue(discovery.getInstances("service-3").isEmpty() || discovery.getInstances("service-3").stream().noneMatch(JQuickGrpcServiceInstance::isHealthy));
        log.info("✓ 所有服务都已注销");
    }
    @Test
    @Order(12)
    @DisplayName("12. 测试并发注册服务")
    void testConcurrentRegister() throws InterruptedException {
        log.info("=== 测试并发注册服务 ===");
        String serviceName = "test-concurrent-service";
        int instanceCount = 5;
        CountDownLatch latch = new CountDownLatch(instanceCount);
        AtomicInteger successCount = new AtomicInteger(0);
        for (int i = 0; i < instanceCount; i++) {
            final int index = i;
            new Thread(() -> {
                try {
                    discovery.registerService(serviceName, "192.168.1." + index, 9000 + index, 1);
                    successCount.incrementAndGet();
                    log.debug(" 实例 {} 注册成功", index);
                } catch (Exception e) {
                    log.error("实例 {} 注册失败: {}", index, e.getMessage());
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        boolean completed = latch.await(30, TimeUnit.SECONDS);
        assertTrue(completed);
        Thread.sleep(3000);
        List<JQuickGrpcServiceInstance> instances = discovery.getInstances(serviceName);
        assertEquals(instanceCount, instances.size(), "所有实例都应该注册成功");
        log.info(" 并发注册完成: {}/{} 成功", successCount.get(), instanceCount);
    }

    @Test
    @Order(13)
    @DisplayName("13. 测试并发获取实例")
    void testConcurrentGetInstances() throws InterruptedException {
        log.info("=== 测试并发获取实例 ===");
        String serviceName = "test-concurrent-get-service";
        discovery.registerService(serviceName, "192.168.1.100", 10086, 1);
        Thread.sleep(2000);
        int threadCount = 20;
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                try {
                    List<JQuickGrpcServiceInstance> instances = discovery.getInstances(serviceName);
                    if (instances != null && !instances.isEmpty()) {
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    log.error("并发获取失败: {}", e.getMessage());
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        boolean completed = latch.await(15, TimeUnit.SECONDS);
        assertTrue(completed);
        assertEquals(threadCount, successCount.get(), "所有线程都应该成功获取实例");

        log.info(" 并发获取测试完成: {}/{} 成功", successCount.get(), threadCount);
    }
    @Test
    @Order(14)
    @DisplayName("14. 测试获取不存在的服务")
    void testGetNonExistentService() {
        log.info("=== 测试获取不存在的服务 ===");
        String nonExistentService = "non-existent-service-" + System.currentTimeMillis();
        List<JQuickGrpcServiceInstance> instances = discovery.getInstances(nonExistentService);
        assertNotNull(instances);
        assertTrue(instances.isEmpty(), "不存在的服务应该返回空列表");
        log.info(" 获取不存在服务返回空列表");
    }

    @Test
    @Order(15)
    void testDuplicateRegistration() throws InterruptedException {
        log.info("=== 测试重复注册相同实例 ===");
        String serviceName = "test-duplicate-service";
        String host = "192.168.1.1";
        int port = 10001;
        discovery.registerService(serviceName, host, port, 1);
        Thread.sleep(2000);
        List<JQuickGrpcServiceInstance> first = discovery.getInstances(serviceName);
        assertEquals(1, first.size());
        log.info(" 第一次注册成功");
        discovery.registerService(serviceName, host, port, 2);
        Thread.sleep(2000);
        List<JQuickGrpcServiceInstance> second = discovery.getInstances(serviceName);
        assertEquals(1, second.size());
        log.info(" 重复注册被正确忽略，实例数: {}", second.size());
    }


    @Test
    @Order(16)
    @DisplayName("16. 测试不同服务隔离")
    void testServiceIsolation() throws InterruptedException {
        log.info("=== 测试不同服务隔离 ===");
        String serviceA = "service-isolation-a";
        String serviceB = "service-isolation-b";
        discovery.registerService(serviceA, "10.0.0.1", 8001, 1);
        discovery.registerService(serviceB, "10.0.0.2", 8002, 2);
        Thread.sleep(3000);
        List<JQuickGrpcServiceInstance> instancesA = discovery.getInstances(serviceA);
        assertEquals(1, instancesA.size());
        assertEquals(serviceA, instancesA.get(0).getServiceName());
        assertEquals(8001, instancesA.get(0).getPort());
        List<JQuickGrpcServiceInstance> instancesB = discovery.getInstances(serviceB);
        assertEquals(1, instancesB.size());
        assertEquals(serviceB, instancesB.get(0).getServiceName());
        assertEquals(8002, instancesB.get(0).getPort());
        log.info(" 服务隔离测试通过");
        log.info("  - {} -> {}", serviceA, instancesA.get(0).getAddress());
        log.info("  - {} -> {}", serviceB, instancesB.get(0).getAddress());
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
