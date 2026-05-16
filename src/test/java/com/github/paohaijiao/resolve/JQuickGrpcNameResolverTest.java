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
package com.github.paohaijiao.resolve;

/**
 * packageName com.github.paohaijiao.resolve
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/16
 */

import com.github.paohaijiao.grpc.discovery.JQuickGrpcServiceDiscovery;
import com.github.paohaijiao.grpc.discovery.impl.JQuickGrpcLocalDiscovery;
import com.github.paohaijiao.grpc.domain.JQuickGrpcServiceInstance;
import com.github.paohaijiao.grpc.resolver.JQuickGrpcNameResolver;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import io.grpc.Status;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class JQuickGrpcNameResolverTest {

    private JQuickGrpcLocalDiscovery discovery;

    private ExecutorService executor;

    private JQuickGrpcNameResolver resolver;

    @Mock
    private NameResolver.Listener2 listener;

    @BeforeEach
    void setUp() {
        discovery = new JQuickGrpcLocalDiscovery();
        executor = Executors.newSingleThreadExecutor();
        resolver = new JQuickGrpcNameResolver("TestService", discovery, executor);
    }

    @AfterEach
    void tearDown() {
        resolver.shutdown();
        executor.shutdown();
        discovery.close();
    }

    @Test
    void testGetServiceAuthority() {
        assertEquals("TestService", resolver.getServiceAuthority());
    }

    @Test
    void testStartWithNoInstances() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        NameResolver.Listener2 testListener = new NameResolver.Listener2() {
            @Override
            public void onResult(NameResolver.ResolutionResult resolutionResult) {
                latch.countDown();
            }
            @Override
            public void onError(Status error) {
                assertEquals(Status.Code.NOT_FOUND, error.getCode());
                assertTrue(error.getDescription().contains("No instances found"));
                latch.countDown();
            }
        };
        resolver.start(testListener);
        boolean completed = latch.await(3, TimeUnit.SECONDS);
        assertTrue(completed, "Expected onError to be called");
    }

    @Test
    void testStartWithInstances() throws Exception {
        discovery.registerService("TestService", "localhost", 9090);
        discovery.registerService("TestService", "localhost", 9091);
        CountDownLatch latch = new CountDownLatch(1);
        List<InetSocketAddress> socketAddresses = new java.util.ArrayList<>();
        NameResolver.Listener2 testListener = new NameResolver.Listener2() {
            @Override
            public void onResult(NameResolver.ResolutionResult resolutionResult) {
                List<EquivalentAddressGroup> addresses = resolutionResult.getAddresses();
                List<InetSocketAddress> extracted = addresses.stream()
                        .flatMap(group -> group.getAddresses().stream())
                        .filter(addr -> addr instanceof InetSocketAddress)
                        .map(addr -> (InetSocketAddress) addr)
                        .collect(java.util.stream.Collectors.toList());
                socketAddresses.addAll(extracted);
                latch.countDown();
            }

            @Override
            public void onError(Status error) {
                System.err.println("Error: " + error);
                latch.countDown();
            }
        };
        resolver.start(testListener);
        boolean completed = latch.await(3, TimeUnit.SECONDS);
        assertTrue(completed, "Expected onResult to be called within 3 seconds");
        assertNotNull(socketAddresses);
        assertEquals(2, socketAddresses.size());
        boolean hasPort9090 = socketAddresses.stream().anyMatch(addr -> addr.getPort() == 9090);
        boolean hasPort9091 = socketAddresses.stream().anyMatch(addr -> addr.getPort() == 9091);
        assertTrue(hasPort9090, "Expected port 9090 to be present");
        assertTrue(hasPort9091, "Expected port 9091 to be present");
    }
    @Test
    void testOnlyHealthyInstancesAreReturned() throws InterruptedException {
        discovery.registerService("TestService", "localhost", 9090);
        discovery.registerService("TestService", "localhost", 9091);
        discovery.updateHealth("TestService", "localhost", 9091, false);
        CountDownLatch latch = new CountDownLatch(1);
        List<InetSocketAddress> socketAddresses = new java.util.ArrayList<>();
        NameResolver.Listener2 testListener = new NameResolver.Listener2() {
            @Override
            public void onResult(NameResolver.ResolutionResult resolutionResult) {
                List<EquivalentAddressGroup> addresses = resolutionResult.getAddresses();
                List<InetSocketAddress> extracted = addresses.stream()
                        .flatMap(group -> group.getAddresses().stream())
                        .filter(addr -> addr instanceof InetSocketAddress)
                        .map(addr -> (InetSocketAddress) addr)
                        .collect(java.util.stream.Collectors.toList());
                socketAddresses.addAll(extracted);
                latch.countDown();
            }

            @Override
            public void onError(Status error) {
                System.err.println("Error: " + error);
                latch.countDown();
            }
        };
        resolver.start(testListener);
        boolean completed = latch.await(3, TimeUnit.SECONDS);
        assertTrue(completed, "Expected onResult to be called within 3 seconds");
        assertEquals(1, socketAddresses.size());
        assertEquals(9090, socketAddresses.get(0).getPort());
    }

    @Test
    void testDynamicInstanceAddition() throws Exception {
        CountDownLatch resultLatch = new CountDownLatch(2);
        java.util.concurrent.atomic.AtomicInteger callCount = new java.util.concurrent.atomic.AtomicInteger(0);
        java.util.List<java.util.List<Integer>> allPorts = new java.util.ArrayList<>();
        NameResolver.Listener2 testListener = new NameResolver.Listener2() {
            @Override
            public void onResult(NameResolver.ResolutionResult resolutionResult) {
                java.util.List<Integer> ports = resolutionResult.getAddresses().stream()
                        .flatMap(group -> group.getAddresses().stream())
                        .filter(addr -> addr instanceof InetSocketAddress)
                        .map(addr -> ((InetSocketAddress) addr).getPort())
                        .collect(java.util.stream.Collectors.toList());
                allPorts.add(ports);
                callCount.incrementAndGet();
                resultLatch.countDown();
            }
            @Override
            public void onError(Status error) {
                System.err.println("Error: " + error);
                resultLatch.countDown();
            }
        };
        resolver.start(testListener);
        Thread.sleep(500);
        discovery.registerService("TestService", "localhost", 9090);
        Thread.sleep(500);
        discovery.registerService("TestService", "localhost", 9091);
        boolean completed = resultLatch.await(5, TimeUnit.SECONDS);
        assertTrue(completed, "Expected at least 2 updates within 5 seconds");
        assertTrue(callCount.get() >= 2, "Expected at least 2 calls, but got " + callCount.get());
        java.util.List<Integer> lastPorts = allPorts.get(allPorts.size() - 1);
        assertTrue(lastPorts.contains(9090));
        assertTrue(lastPorts.contains(9091));
    }

    @Test
    void testDynamicInstanceRemoval() throws Exception {
        discovery.registerService("TestService", "localhost", 9090);
        discovery.registerService("TestService", "localhost", 9091);
        CountDownLatch initialLatch = new CountDownLatch(1);
        CountDownLatch removalLatch = new CountDownLatch(1);
        List<List<InetSocketAddress>> snapshots = new java.util.ArrayList<>();
        NameResolver.Listener2 testListener = new NameResolver.Listener2() {
            @Override
            public void onResult(NameResolver.ResolutionResult resolutionResult) {
                List<InetSocketAddress> addresses = extractSocketAddresses(resolutionResult.getAddresses());
                snapshots.add(new java.util.ArrayList<>(addresses));
                int size = addresses.size();
                if (size == 2) {
                    initialLatch.countDown();
                } else if (size == 1) {
                    removalLatch.countDown();
                }
            }

            @Override
            public void onError(Status error) {
                initialLatch.countDown();
                removalLatch.countDown();
            }
        };
        resolver.start(testListener);
        assertTrue(initialLatch.await(3, TimeUnit.SECONDS), "Should reach initial state with 2 instances");
        List<InetSocketAddress> initialSnapshot = snapshots.stream()
                .filter(s -> s.size() == 2)
                .findFirst()
                .orElse(null);
        assertNotNull(initialSnapshot);
        assertEquals(2, initialSnapshot.size());
        discovery.unregisterService("TestService", "localhost", 9090);
        assertTrue(removalLatch.await(3, TimeUnit.SECONDS), "Should reach state with 1 instance after removal");
        List<InetSocketAddress> afterRemoval = snapshots.stream()
                .filter(s -> s.size() == 1)
                .reduce((first, second) -> second)
                .orElse(null);
        assertNotNull(afterRemoval);
        assertEquals(1, afterRemoval.size());
        assertEquals(9091, afterRemoval.get(0).getPort());
        System.out.println("Change history:");
        for (int i = 0; i < snapshots.size(); i++) {
            List<Integer> ports = snapshots.get(i).stream()
                    .map(InetSocketAddress::getPort)
                    .collect(java.util.stream.Collectors.toList());
            System.out.println("  Step " + i + ": " + ports);
        }
    }




    private List<InetSocketAddress> extractSocketAddresses(List<EquivalentAddressGroup> addressGroups) {
        return addressGroups.stream()
                .flatMap(group -> group.getAddresses().stream())
                .filter(addr -> addr instanceof InetSocketAddress)
                .map(addr -> (InetSocketAddress) addr)
                .collect(java.util.stream.Collectors.toList());
    }
}
