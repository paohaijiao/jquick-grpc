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

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class JQuickGrpcEtcdDiscoveryPerformanceTest {

    private JQuickGrpcEtcdDiscovery discovery;

    private static final String PERFORMANCE_SERVICE = "perf-test-service";

    @BeforeEach
    void setUp() {
        discovery = new JQuickGrpcEtcdDiscovery("http://192.168.32.173:2379");
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
                JQuickGrpcEtcdDiscovery d = new JQuickGrpcEtcdDiscovery("http://192.168.32.173:2379");
                discoveries.add(d);
                d.registerService(PERFORMANCE_SERVICE, "192.168.1." + i, 9000 + i, 1);
            }
            // 等待所有注册完成
            Thread.sleep(2000);
            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            List<JQuickGrpcServiceInstance> instances = discovery.getInstances(PERFORMANCE_SERVICE);
            System.out.println("注册 " + instanceCount + " 个实例耗时: " + duration + "ms");
            System.out.println("实际发现实例数: " + instances.size());
            Assertions.assertEquals(instanceCount, instances.size());
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
}
