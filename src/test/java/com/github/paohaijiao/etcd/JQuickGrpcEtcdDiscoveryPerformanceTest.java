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

import com.github.paohaijiao.grpc.discovery.impl.JQuickGrpcEtcdDiscovery;
import com.github.paohaijiao.grpc.domain.JQuickGrpcServiceInstance;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Integration test: requires an external etcd server; disabled by default so CI and
 * local builds without etcd stay green. Remove @Disabled to run against a live cluster.
 */
@Disabled("Requires an external etcd server / 需要外部 etcd 服务器")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class JQuickGrpcEtcdDiscoveryPerformanceTest {

    private JQuickGrpcEtcdDiscovery discovery;

    private static final String PERFORMANCE_SERVICE = "perf-test-service";

    /**
     * 本类所有 discovery 客户端共用的 etcd 地址，默认指向仓库 start-etcd.sh 启动的本地 etcd，
     * 可通过 -Detcd.endpoints=http://host:2379 覆盖。
     * 修复要点：注册端与查询端必须指向同一 endpoint（并支持统一覆盖）。此前各客户端
     * 硬编码不同/不可达的地址（如 192.168.32.173），导致注册的数据查询端永远看不到，
     * getInstances 恒为空，且注册因连接失败提前抛出、startKeepAlive 从未执行。
     * The single etcd endpoint shared by every discovery client in this class; defaults to the
     * local etcd launched by start-etcd.sh, overridable with -Detcd.endpoints=http://host:2379.
     * Fix: registrars and lookups must target the same reachable endpoint. Previously clients
     * hard-coded different/unreachable addresses (e.g. 192.168.32.173), so registered data was
     * never visible to the lookup client (empty getInstances) and registration aborted before
     * startKeepAlive ever ran.
     */
    private static final String ETCD_ENDPOINTS = System.getProperty("etcd.endpoints", "http://127.0.0.1:2379");

    @BeforeEach
    void setUp() {
        discovery = new JQuickGrpcEtcdDiscovery(ETCD_ENDPOINTS);
    }

    @AfterEach
    void tearDown() {
        if (discovery != null) {
            discovery.unregisterService();
            discovery.close();
        }
    }

    @Test
    @Order(1)
    void testBatchRegister() {
        int instanceCount = 50;
        long startTime = System.currentTimeMillis();
        List<JQuickGrpcEtcdDiscovery> discoveries = new ArrayList<>();
        try {
            for (int i = 0; i < instanceCount; i++) {
                // 注册端与查询端必须使用同一个 etcd endpoint，否则数据互不可见
                JQuickGrpcEtcdDiscovery d = new JQuickGrpcEtcdDiscovery(ETCD_ENDPOINTS);
                discoveries.add(d);
                d.registerService(PERFORMANCE_SERVICE, "127.0.0.1", 9000 + i, 1);
            }
            // 轮询等待实例在 etcd 中可见，替代固定 sleep，兼容 etcd 集群副本同步延迟
            List<JQuickGrpcServiceInstance> instances =
                    awaitInstances(PERFORMANCE_SERVICE, instanceCount, 10000);
            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            System.out.println("注册 " + instanceCount + " 个实例耗时: " + duration + "ms");
            System.out.println("实际发现实例数: " + instances.size());
            Assertions.assertEquals(instanceCount, instances.size(), "批量注册后应能发现全部实例");
        } catch (Exception e) {
            Assertions.fail("批量注册失败: " + e.getMessage());
        } finally {
            for (JQuickGrpcEtcdDiscovery d : discoveries) {
                try {
                    d.unregisterService();
                    d.close();
                } catch (Exception e) {
                }
            }
        }
    }

    /**
     * 轮询服务发现直至实例数达标或超时。
     * Polls discovery until the expected number of instances appears or the timeout elapses.
     */
    private List<JQuickGrpcServiceInstance> awaitInstances(String serviceName, int expected, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        List<JQuickGrpcServiceInstance> instances = new ArrayList<>();
        while (System.currentTimeMillis() < deadline) {
            instances = discovery.getInstances(serviceName);
            if (instances.size() >= expected) {
                return instances;
            }
            Thread.sleep(200);
        }
        return instances;
    }

    @Test
    @Order(2)
    void testConcurrentSubscribe() throws InterruptedException {
        int subscribeCount = 100;
        CountDownLatch latch = new CountDownLatch(subscribeCount);
        AtomicLong totalTime = new AtomicLong(0);
        for (int i = 0; i < subscribeCount; i++) {
            final int index = i;
            new Thread(() -> {
                long start = System.nanoTime();
                discovery.subscribe(PERFORMANCE_SERVICE + index, (serviceName, instances) -> {});
                long end = System.nanoTime();
                totalTime.addAndGet(end - start);
                latch.countDown();
            }).start();
        }

        boolean completed = latch.await(30, TimeUnit.SECONDS);
        Assertions.assertTrue(completed);
        long avgTimeMicros = totalTime.get() / subscribeCount / 1000;
        System.out.println("✓ 平均订阅耗时: " + avgTimeMicros + "μs");
        System.out.println("✓ 总订阅数: " + subscribeCount);
    }

    @Test
    @Order(3)
    @DisplayName("注册后实例存活超过 lease TTL，证明 keepalive 生效 / instance outlives the lease TTL, proving keep-alive runs")
    void testKeepAliveKeepsRegistrationAliveBeyondLeaseTtl() throws Exception {
        final String keepAliveService = "keepalive-test-service";
        JQuickGrpcEtcdDiscovery registrar = new JQuickGrpcEtcdDiscovery(ETCD_ENDPOINTS, null, null, "/grpc/services", 3);
        try {
            registrar.registerService(keepAliveService, "127.0.0.1", 9300, 1);
            // Wait beyond the lease TTL; startKeepAlive should renew it roughly every second.
            Thread.sleep(5000);
            List<JQuickGrpcServiceInstance> instances = discovery.getInstances(keepAliveService);
            Assertions.assertEquals(1, instances.size(),
                    "超过 lease TTL 后实例仍应存在；若 keepalive 未生效，etcd 会在 ~3s 回收该键导致查询为空");
            // 对照：注销后键应立即被删除
            registrar.unregisterService();
            Thread.sleep(500);
            Assertions.assertTrue(discovery.getInstances(keepAliveService).isEmpty(),
                    "注销后实例应从服务列表中移除");
        } finally {
            try {
                registrar.unregisterService();
            } catch (Exception ignored) {
                // best effort cleanup
            }
            registrar.close();
        }
    }
}
