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
package com.github.paohaijiao.grpc.discovery.impl;

import com.github.paohaijiao.console.JConsole;
import com.github.paohaijiao.grpc.discovery.JQuickGrpcServiceDiscovery;
import com.github.paohaijiao.grpc.domain.JQuickGrpcServiceInstance;
import com.github.paohaijiao.grpc.metadata.JQuickServiceInstanceMetrics;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 基于内存 Map 的本地服务发现实现
 * 适用于开发、测试环境，无需外部依赖
 */
public class JQuickGrpcLocalDiscovery implements JQuickGrpcServiceDiscovery {

    private static final JConsole log = JConsole.getInstance();

    private final Map<String, List<JQuickGrpcServiceInstance>> serviceMap;

    private final Map<String, List<ServiceChangeListener>> listeners;

    private final Map<String, String> instanceToServiceMap;

    private final Map<String, JQuickServiceInstanceMetrics> metricsMap;

    private final ExecutorService executor;

    private final AtomicBoolean closed;

    private final AtomicInteger idGenerator;

    public JQuickGrpcLocalDiscovery() {
        this.serviceMap = new ConcurrentHashMap<>();
        this.listeners = new ConcurrentHashMap<>();
        this.instanceToServiceMap = new ConcurrentHashMap<>();
        this.metricsMap = new ConcurrentHashMap<>();
        this.executor = Executors.newCachedThreadPool();
        this.closed = new AtomicBoolean(false);
        this.idGenerator = new AtomicInteger(0);
        log.info("Local memory-based discovery created");
    }

    @Override
    public List<JQuickGrpcServiceInstance> getInstances(String serviceName) {
        if (closed.get()) {
            return Collections.emptyList();
        }
        List<JQuickGrpcServiceInstance> instances = serviceMap.get(serviceName);
        if (instances == null || instances.isEmpty()) {
            log.debug("No instances found for service: {}", serviceName);
            return Collections.emptyList();
        }
        List<JQuickGrpcServiceInstance> healthyInstances = instances.stream()
                .filter(JQuickGrpcServiceInstance::isHealthy)
                .collect(Collectors.toList());

        log.debug("Found {} healthy instances for service: {}", healthyInstances.size(), serviceName);
        return healthyInstances;
    }

    @Override
    public void subscribe(String serviceName, ServiceChangeListener listener) {
        if (closed.get()) {
            return;
        }
        listeners.computeIfAbsent(serviceName, k -> new CopyOnWriteArrayList<>()).add(listener);
        log.info("Subscribed to service changes: {}", serviceName);
        List<JQuickGrpcServiceInstance> instances = getInstances(serviceName);
        listener.onChange(serviceName, instances);
    }

    @Override
    public void unsubscribe(String serviceName, ServiceChangeListener listener) {
        List<ServiceChangeListener> serviceListeners = listeners.get(serviceName);
        if (serviceListeners != null) {
            serviceListeners.remove(listener);
            if (serviceListeners.isEmpty()) {
                listeners.remove(serviceName);
                log.info("Unsubscribed from service changes: {}", serviceName);
            }
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        log.info("Closing local discovery...");
        serviceMap.clear();
        listeners.clear();
        instanceToServiceMap.clear();
        metricsMap.clear();
        executor.shutdown();
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log.info("Local discovery closed");
    }

    /**
     * 注册服务实例
     */
    public void registerService(String serviceName, String host, int port) {
        registerService(serviceName, host, port, 1, null);
    }

    /**
     * 注册服务实例
     */
    public void registerService(String serviceName, String host, int port, int weight) {
        registerService(serviceName, host, port, weight, null);
    }

    /**
     * 注册服务实例（带 Metrics）
     */
    public void registerService(String serviceName, String host, int port, int weight, JQuickServiceInstanceMetrics metrics) {
        if (closed.get()) {
            log.warn("Cannot register service, discovery is closed");
            return;
        }
        String instanceId = generateInstanceId(serviceName, host, port);
        if (instanceToServiceMap.containsKey(instanceId)) {
            log.warn("Service instance already registered: {}", instanceId);
            return;
        }
        JQuickGrpcServiceInstance instance = new JQuickGrpcServiceInstance(serviceName, host, port);
        instance.setWeight(weight);
        instance.setHealthy(true);
        if (metrics != null) {
            instance.setMetrics(metrics);
            metricsMap.put(instanceId, metrics);
        }
        instanceToServiceMap.put(instanceId, serviceName);
        serviceMap.computeIfAbsent(serviceName, k -> new CopyOnWriteArrayList<>()).add(instance);
        log.info("Registered service instance: {} at {}:{} (weight={})", serviceName, host, port, weight);
        notifyChange(serviceName);
    }

    /**
     * 注销服务实例
     */
    public void unregisterService(String serviceName, String host, int port) {
        String instanceId = generateInstanceId(serviceName, host, port);
        if (!instanceToServiceMap.containsKey(instanceId)) {
            log.warn("Service instance not registered: {}", instanceId);
            return;
        }
        List<JQuickGrpcServiceInstance> instances = serviceMap.get(serviceName);
        if (instances != null) {
            instances.removeIf(instance -> instance.getHost().equals(host) && instance.getPort() == port);
            if (instances.isEmpty()) {
                serviceMap.remove(serviceName);
            }
        }
        instanceToServiceMap.remove(instanceId);
        metricsMap.remove(instanceId);
        log.info("Unregistered service instance: {}", instanceId);
        notifyChange(serviceName);
    }

    /**
     * 注销所有服务
     */
    public void unregisterAllServices() {
        log.info("Unregistering all services...");
        Set<String> serviceNames = new HashSet<>(serviceMap.keySet());
        for (String serviceName : serviceNames) {
            List<JQuickGrpcServiceInstance> instances = serviceMap.get(serviceName);
            if (instances != null) {
                for (JQuickGrpcServiceInstance instance : instances) {
                    String instanceId = generateInstanceId(serviceName, instance.getHost(), instance.getPort());
                    instanceToServiceMap.remove(instanceId);
                    metricsMap.remove(instanceId);
                }
            }
            serviceMap.remove(serviceName);
            notifyChange(serviceName);
        }
        log.info("All services unregistered");
    }

    /**
     * 更新健康状态
     */
    public void updateHealth(String serviceName, String host, int port, boolean healthy) {
        String instanceId = generateInstanceId(serviceName, host, port);
        List<JQuickGrpcServiceInstance> instances = serviceMap.get(serviceName);
        if (instances != null) {
            for (JQuickGrpcServiceInstance instance : instances) {
                if (instance.getHost().equals(host) && instance.getPort() == port) {
                    instance.setHealthy(healthy);
                    log.info("Updated health status for instance: {} to {}", instanceId, healthy);
                    notifyChange(serviceName);
                    return;
                }
            }
        }
        log.warn("Instance not found for health update: {}", instanceId);
    }

    /**
     * 更新 Metrics
     */
    public void updateMetrics(String serviceName, String host, int port, JQuickServiceInstanceMetrics metrics) {
        String instanceId = generateInstanceId(serviceName, host, port);
        List<JQuickGrpcServiceInstance> instances = serviceMap.get(serviceName);
        if (instances != null) {
            for (JQuickGrpcServiceInstance instance : instances) {
                if (instance.getHost().equals(host) && instance.getPort() == port) {
                    instance.setMetrics(metrics);
                    metricsMap.put(instanceId, metrics);
                    log.debug("Updated metrics for instance: {}", instanceId);
                    return;
                }
            }
        }
        log.warn("Instance not found for metrics update: {}", instanceId);
    }

    /**
     * 更新权重
     */
    public void updateWeight(String serviceName, String host, int port, int weight) {
        List<JQuickGrpcServiceInstance> instances = serviceMap.get(serviceName);
        if (instances != null) {
            for (JQuickGrpcServiceInstance instance : instances) {
                if (instance.getHost().equals(host) && instance.getPort() == port) {
                    instance.setWeight(weight);
                    log.info("Updated weight for instance: {} to {}", instance.getAddress(), weight);
                    notifyChange(serviceName);
                    return;
                }
            }
        }
    }

    /**
     * 获取所有注册的服务名称
     */
    public Set<String> getAllServiceNames() {
        return new HashSet<>(serviceMap.keySet());
    }

    /**
     * 获取指定服务的所有实例（包括不健康的）
     */
    public List<JQuickGrpcServiceInstance> getAllInstances(String serviceName) {
        List<JQuickGrpcServiceInstance> instances = serviceMap.get(serviceName);
        if (instances == null) {
            return Collections.emptyList();
        }
        return new ArrayList<>(instances);
    }

    /**
     * 获取统计信息
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("serviceCount", serviceMap.size());
        stats.put("instanceCount", instanceToServiceMap.size());
        stats.put("listenerCount", listeners.values().stream().mapToInt(List::size).sum());
        stats.put("closed", closed.get());
        Map<String, Integer> serviceStats = new HashMap<>();
        for (Map.Entry<String, List<JQuickGrpcServiceInstance>> entry : serviceMap.entrySet()) {
            serviceStats.put(entry.getKey(), entry.getValue().size());
        }
        stats.put("serviceDetails", serviceStats);
        return stats;
    }

    /**
     * 清空所有数据
     */
    public void clear() {
        log.info("Clearing all service data...");
        serviceMap.clear();
        instanceToServiceMap.clear();
        metricsMap.clear();
        for (String serviceName : new HashSet<>(listeners.keySet())) {
            notifyChange(serviceName);
        }
    }

    /**
     * 生成实例 ID
     */
    private String generateInstanceId(String serviceName, String host, int port) {
        return serviceName + "|" + host + ":" + port;
    }

    /**
     * 通知服务变更
     */
    private void notifyChange(String serviceName) {
        List<ServiceChangeListener> serviceListeners = listeners.get(serviceName);
        if (serviceListeners == null || serviceListeners.isEmpty()) {
            return;
        }
        executor.submit(() -> {
            List<JQuickGrpcServiceInstance> instances = getInstances(serviceName);
            for (ServiceChangeListener listener : serviceListeners) {
                try {
                    listener.onChange(serviceName, instances);
                    log.debug("Notified listener for service: {}, instances: {}", serviceName, instances.size());
                } catch (Exception e) {
                    log.error("Error notifying listener for service: {}", serviceName, e);
                }
            }
        });
    }
}
