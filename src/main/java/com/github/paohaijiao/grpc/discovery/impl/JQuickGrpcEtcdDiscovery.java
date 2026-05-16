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
import io.etcd.jetcd.*;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.watch.WatchResponse;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Etcd 服务发现实现
 * 支持服务注册、发现、健康检查、Metrics 上报
 */
public class JQuickGrpcEtcdDiscovery implements JQuickGrpcServiceDiscovery {

    private static final JConsole log = JConsole.getInstance();

    private static final Gson gson = new GsonBuilder().create();

    private static final String DEFAULT_ROOT_PATH = "/grpc/services";

    private static final long DEFAULT_LEASE_TTL_SECONDS = 30;

    private Client etcdClient;

    private KV kvClient;

    private Watch watchClient;

    private Lease leaseClient;

    private final String endpoints;

    private final String username;

    private final String password;

    private final String rootPath;

    private final long leaseTtlSeconds;

    private final Map<String, List<ServiceChangeListener>> listeners;

    private final Map<String, List<JQuickGrpcServiceInstance>> instanceCache;

    private final Map<String, Watch.Watcher> watchers;

    private final Map<String, Long> leaseIds;

    private final ExecutorService executor;

    private final AtomicBoolean closed;

    private final AtomicBoolean initialized;

    private String registeredInstanceId;

    private String registeredServiceName;

    private long registeredLeaseId;

    private ScheduledExecutorService keepAliveExecutor;

    public JQuickGrpcEtcdDiscovery() {
        this("http://localhost:2379");
    }

    public JQuickGrpcEtcdDiscovery(String endpoints) {
        this(endpoints, null, null, DEFAULT_ROOT_PATH, DEFAULT_LEASE_TTL_SECONDS);
    }

    public JQuickGrpcEtcdDiscovery(String endpoints, String username, String password) {
        this(endpoints, username, password, DEFAULT_ROOT_PATH, DEFAULT_LEASE_TTL_SECONDS);
    }

    public JQuickGrpcEtcdDiscovery(String endpoints, String username, String password, String rootPath, long leaseTtlSeconds) {
        this.endpoints = endpoints;
        this.username = username;
        this.password = password;
        this.rootPath = rootPath;
        this.leaseTtlSeconds = leaseTtlSeconds;
        this.listeners = new ConcurrentHashMap<>();
        this.instanceCache = new ConcurrentHashMap<>();
        this.watchers = new ConcurrentHashMap<>();
        this.leaseIds = new ConcurrentHashMap<>();
        this.executor = Executors.newCachedThreadPool();
        this.closed = new AtomicBoolean(false);
        this.initialized = new AtomicBoolean(false);
        log.info("Creating Etcd discovery with endpoints: {}", endpoints);
        initializeClient();
    }

    /**
     * 初始化 Etcd 客户端
     */
    private synchronized void initializeClient() {
        if (initialized.get()) {
            return;
        }
        try {
            ClientBuilder builder = Client.builder().endpoints(endpoints.split(","));
            if (username != null && !username.isEmpty() && password != null) {
                builder.user(ByteSequence.from(username, StandardCharsets.UTF_8));
                builder.password(ByteSequence.from(password, StandardCharsets.UTF_8));
            }
            this.etcdClient = builder.build();
            this.kvClient = etcdClient.getKVClient();
            this.watchClient = etcdClient.getWatchClient();
            this.leaseClient = etcdClient.getLeaseClient();
            initialized.set(true);
            log.info("Etcd client initialized successfully with endpoints: {}", endpoints);
        } catch (Exception e) {
            log.error("Failed to initialize Etcd client, endpoints: {}", endpoints, e);
            throw new RuntimeException("Failed to initialize Etcd client", e);
        }
    }

    /**
     * 确保客户端已初始化（用于可能未初始化的情况）
     */
    private void ensureInitialized() {
        if (!initialized.get() && !closed.get()) {
            initializeClient();
        }
        if (!initialized.get()) {
            throw new IllegalStateException("Etcd client not initialized");
        }
    }

    @Override
    public List<JQuickGrpcServiceInstance> getInstances(String serviceName) {
        if (closed.get() || !initialized.get()) {
            log.debug("Cannot get instances, client closed or not initialized: {}", serviceName);
            return Collections.emptyList();
        }
        List<JQuickGrpcServiceInstance> cached = instanceCache.get(serviceName);
        if (cached != null && !cached.isEmpty()) {
            log.debug("Returning {} cached instances for service: {}", cached.size(), serviceName);
            return new ArrayList<>(cached);
        }
        try {
            ensureInitialized();
            String prefix = getServicePath(serviceName);
            GetResponse response = kvClient.get(ByteSequence.from(prefix, StandardCharsets.UTF_8), GetOption.newBuilder().withPrefix(ByteSequence.from(prefix, StandardCharsets.UTF_8)).build()).get(5, TimeUnit.SECONDS);
            List<JQuickGrpcServiceInstance> instances = response.getKvs().stream()
                    .map(kv -> parseInstance(kv.getValue().toString(StandardCharsets.UTF_8)))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            instanceCache.put(serviceName, new ArrayList<>(instances));
            log.info("Discovered {} instances for service: {}", instances.size(), serviceName);
            return instances;
        } catch (Exception e) {
            log.error("Failed to get instances from etcd for service: {}", serviceName, e);
            return Collections.emptyList();
        }
    }

    @Override
    public void subscribe(String serviceName, ServiceChangeListener listener) {
        if (closed.get()) {
            log.warn("Cannot subscribe, client closed: {}", serviceName);
            return;
        }
        listeners.computeIfAbsent(serviceName, k -> new ArrayList<>()).add(listener);
        log.info("Subscribed to service changes: {}", serviceName);
        // 如果还没有为该服务创建 watcher，则创建
        if (!watchers.containsKey(serviceName)) {
            createWatcher(serviceName);
        }
        // 立即触发一次，推送当前实例列表
        List<JQuickGrpcServiceInstance> instances = getInstances(serviceName);
        listener.onChange(serviceName, instances);
    }

    @Override
    public void unsubscribe(String serviceName, ServiceChangeListener listener) {
        List<ServiceChangeListener> serviceListeners = listeners.get(serviceName);
        if (serviceListeners != null) {
            serviceListeners.remove(listener);
            if (serviceListeners.isEmpty()) {
                // 没有监听者了，关闭 watcher
                Watch.Watcher watcher = watchers.remove(serviceName);
                if (watcher != null) {
                    watcher.close();
                    log.info("Unsubscribed from service changes: {}", serviceName);
                }
            }
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        log.info("Closing Etcd discovery...");
        // 关闭 keep alive 线程池
        if (keepAliveExecutor != null && !keepAliveExecutor.isShutdown()) {
            keepAliveExecutor.shutdown();
        }
        // 注销服务注册（如果有）
        unregisterService();
        // 关闭所有 watcher
        watchers.values().forEach(watcher -> {
            try {
                watcher.close();
            } catch (Exception e) {
                log.warn("Error closing watcher", e);
            }
        });
        watchers.clear();
        // 关闭客户端
        if (etcdClient != null) {
            etcdClient.close();
        }
        // 关闭线程池
        executor.shutdown();
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        initialized.set(false);
        log.info("Etcd discovery closed");
    }

    /**
     * 创建服务监听器
     */
    private void createWatcher(String serviceName) {
        if (closed.get() || !initialized.get()) {
            return;
        }
        try {
            ensureInitialized();
            String prefix = getServicePath(serviceName);
            Watch.Watcher watcher = watchClient.watch(
                    ByteSequence.from(prefix, StandardCharsets.UTF_8),
                    new Watch.Listener() {
                        @Override
                        public void onNext(WatchResponse response) {
                            handleWatchResponse(serviceName, response);
                        }
                        @Override
                        public void onError(Throwable throwable) {
                            log.error("Watch error for service: {}", serviceName, throwable);
                            watchers.remove(serviceName);
                            // 重试创建 watcher
                            executor.submit(() -> {
                                try {
                                    Thread.sleep(5000);
                                    if (!closed.get() && initialized.get()) {
                                        createWatcher(serviceName);
                                    }
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                            });
                        }

                        @Override
                        public void onCompleted() {
                            log.info("Watch completed for service: {}", serviceName);
                            watchers.remove(serviceName);
                        }
                    }
            );
            watchers.put(serviceName, watcher);
            log.info("Watcher created for service: {}", serviceName);
        } catch (Exception e) {
            log.error("Failed to create watcher for service: {}", serviceName, e);
        }
    }

    /**
     * 处理 watch 响应
     */
    private void handleWatchResponse(String serviceName, WatchResponse response) {
        log.debug("Watch response received for service: {}", serviceName);
        List<JQuickGrpcServiceInstance> newInstances = getInstances(serviceName);
        instanceCache.put(serviceName, new ArrayList<>(newInstances));
        // 通知所有监听器
        List<ServiceChangeListener> serviceListeners = listeners.get(serviceName);
        if (serviceListeners != null) {
            for (ServiceChangeListener listener : serviceListeners) {
                try {
                    listener.onChange(serviceName, newInstances);
                    log.debug("Notified listener for service: {}, instances: {}", serviceName, newInstances.size());
                } catch (Exception e) {
                    log.error("Error notifying listener for service: {}", serviceName, e);
                }
            }
        }
    }

    /**
     * 注册服务实例（供服务端使用）
     */
    public void registerService(String serviceName, String host, int port) {
        registerService(serviceName, host, port, 1, null);
    }

    /**
     * 注册服务实例（供服务端使用）
     */
    public void registerService(String serviceName, String host, int port, int weight) {
        registerService(serviceName, host, port, weight, null);
    }

    /**
     * 注册服务实例（包含 Metrics）
     */
    public void registerService(String serviceName, String host, int port, int weight, JQuickServiceInstanceMetrics metrics) {
        if (registeredInstanceId != null) {
            log.warn("Service already registered: {}", registeredServiceName);
            return;
        }
        try {
            ensureInitialized();
            // 创建 lease
            long leaseId = leaseClient.grant(leaseTtlSeconds).get(5, TimeUnit.SECONDS).getID();
            // 创建服务实例
            JQuickGrpcServiceInstance instance = new JQuickGrpcServiceInstance(serviceName, host, port);
            instance.setWeight(weight);
            instance.setHealthy(true);
            if (metrics != null) {
                instance.setMetrics(metrics);
            }
            String instanceId = UUID.randomUUID().toString();
            String key = getInstancePath(serviceName, instanceId);
            String value = serializeInstance(instance);
            // 注册到 etcd（带 lease）
            kvClient.put(ByteSequence.from(key, StandardCharsets.UTF_8), ByteSequence.from(value, StandardCharsets.UTF_8), PutOption.newBuilder().withLeaseId(leaseId).build()).get(5, TimeUnit.SECONDS);
            // 启动 keep alive
            startKeepAlive(leaseId, serviceName);
            registeredInstanceId = instanceId;
            registeredServiceName = serviceName;
            registeredLeaseId = leaseId;
            leaseIds.put(instanceId, leaseId);
            log.info("Registered service instance: {}/{} at {}:{} (weight={})", serviceName, instanceId, host, port, weight);
            if (metrics != null) {
                log.debug("Instance metrics: CPU={}, Memory={}, ActiveRequests={}", metrics.getCpuUsage(), metrics.getMemoryUsage(), metrics.getActiveRequests());
            }
        } catch (Exception e) {
            log.error("Failed to register service: {}", serviceName, e);
            throw new RuntimeException("Failed to register service", e);
        }
    }

    /**
     * 启动 Keep Alive 心跳
     */
    private void startKeepAlive(long leaseId, String serviceName) {
        keepAliveExecutor = Executors.newSingleThreadScheduledExecutor();
        keepAliveExecutor.scheduleAtFixedRate(() -> {
            try {
                if (!closed.get() && initialized.get() && leaseClient != null) {
                    leaseClient.keepAliveOnce(leaseId).get(3, TimeUnit.SECONDS);
                    log.debug("Keep alive sent for service: {}", serviceName);
                }
            } catch (Exception e) {
                log.warn("Failed to send keep alive for service: {}", serviceName, e);
            }
        }, 5, leaseTtlSeconds / 3, TimeUnit.SECONDS);
    }

    /**
     * 更新服务实例的 Metrics
     */
    public void updateMetrics(JQuickServiceInstanceMetrics metrics) {
        if (registeredInstanceId == null || !initialized.get()) {
            log.warn("Cannot update metrics, instance not registered");
            return;
        }
        try {
            String key = getInstancePath(registeredServiceName, registeredInstanceId);
            GetResponse response = kvClient.get(ByteSequence.from(key, StandardCharsets.UTF_8)).get(3, TimeUnit.SECONDS);
            if (response.getKvs().isEmpty()) {
                log.warn("Instance not found in etcd: {}", key);
                return;
            }
            JQuickGrpcServiceInstance instance = parseInstance(response.getKvs().get(0).getValue().toString(StandardCharsets.UTF_8));
            if (instance != null) {
                instance.setMetrics(metrics);
                String value = serializeInstance(instance);
                kvClient.put(ByteSequence.from(key, StandardCharsets.UTF_8), ByteSequence.from(value, StandardCharsets.UTF_8), PutOption.newBuilder().withLeaseId(registeredLeaseId).build()).get(3, TimeUnit.SECONDS);
                log.debug("Updated metrics for service: {}", registeredServiceName);
            }
        } catch (Exception e) {
            log.error("Failed to update metrics", e);
        }
    }

    /**
     * 注销服务实例
     */
    public void unregisterService() {
        if (registeredInstanceId == null) {
            return;
        }

        try {
            if (initialized.get() && kvClient != null) {
                String key = getInstancePath(registeredServiceName, registeredInstanceId);
                kvClient.delete(ByteSequence.from(key, StandardCharsets.UTF_8)).get(3, TimeUnit.SECONDS);
                // 取消 lease
                if (leaseClient != null && registeredLeaseId != 0) {
                    leaseClient.revoke(registeredLeaseId).get(3, TimeUnit.SECONDS);
                }
            }
            // 停止 keep alive
            if (keepAliveExecutor != null && !keepAliveExecutor.isShutdown()) {
                keepAliveExecutor.shutdown();
            }
            log.info("Unregistered service instance: {}/{}", registeredServiceName, registeredInstanceId);
        } catch (Exception e) {
            log.error("Failed to unregister service", e);
        } finally {
            registeredInstanceId = null;
            registeredServiceName = null;
            registeredLeaseId = 0;
        }
    }

    /**
     * 更新服务实例的健康状态
     */
    public void updateHealth(boolean healthy) {
        if (registeredInstanceId == null || !initialized.get()) {
            log.warn("Cannot update health, instance not registered");
            return;
        }
        try {
            String key = getInstancePath(registeredServiceName, registeredInstanceId);
            GetResponse response = kvClient.get(ByteSequence.from(key, StandardCharsets.UTF_8)).get(3, TimeUnit.SECONDS);
            if (response.getKvs().isEmpty()) {
                log.warn("Instance not found in etcd: {}", key);
                return;
            }
            JQuickGrpcServiceInstance instance = parseInstance(response.getKvs().get(0).getValue().toString(StandardCharsets.UTF_8));
            if (instance != null) {
                instance.setHealthy(healthy);
                String value = serializeInstance(instance);
                kvClient.put(ByteSequence.from(key, StandardCharsets.UTF_8), ByteSequence.from(value, StandardCharsets.UTF_8), PutOption.newBuilder().withLeaseId(registeredLeaseId).build()).get(3, TimeUnit.SECONDS);
                log.info("Updated health status for service: {} to {}", registeredServiceName, healthy);
            }
        } catch (Exception e) {
            log.error("Failed to update health status", e);
        }
    }
    private String getServicePath(String serviceName) {
        return rootPath + "/" + serviceName + "/";
    }

    private String getInstancePath(String serviceName, String instanceId) {
        return rootPath + "/" + serviceName + "/" + instanceId;
    }

    /**
     * 序列化实例 - 包含完整的 serviceName 和 Metrics
     */
    private String serializeInstance(JQuickGrpcServiceInstance instance) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("serviceName", instance.getServiceName());
        data.put("host", instance.getHost());
        data.put("port", instance.getPort());
        data.put("weight", instance.getWeight());
        data.put("healthy", instance.isHealthy());
        if (instance.getMetrics() != null) {
            JQuickServiceInstanceMetrics metrics = instance.getMetrics();
            Map<String, Object> metricsData = new LinkedHashMap<>();
            metricsData.put("cpuUsage", metrics.getCpuUsage());
            metricsData.put("memoryUsage", metrics.getMemoryUsage());
            metricsData.put("activeRequests", metrics.getActiveRequests());
            metricsData.put("queueSize", metrics.getQueueSize());
            metricsData.put("lastReportTime", metrics.getLastReportTime());
            data.put("metrics", metricsData);
        }
        return gson.toJson(data);
    }

    /**
     * 解析实例 - 包含完整的 serviceName 和 Metrics
     */
    private JQuickGrpcServiceInstance parseInstance(String json) {
        try {
            Type type = new TypeToken<Map<String, Object>>() {}.getType();
            Map<String, Object> data = gson.fromJson(json, type);
            String serviceName = (String) data.get("serviceName");
            String host = (String) data.get("host");
            int port = ((Number) data.get("port")).intValue();
            int weight = ((Number) data.get("weight")).intValue();
            boolean healthy = (Boolean) data.get("healthy");
            JQuickGrpcServiceInstance instance = new JQuickGrpcServiceInstance(serviceName, host, port);
            instance.setWeight(weight);
            instance.setHealthy(healthy);
            // 解析 Metrics
            @SuppressWarnings("unchecked")
            Map<String, Object> metricsData = (Map<String, Object>) data.get("metrics");
            if (metricsData != null) {
                JQuickServiceInstanceMetrics metrics = new JQuickServiceInstanceMetrics();
                metrics.setCpuUsage(((Number) metricsData.get("cpuUsage")).doubleValue());
                metrics.setMemoryUsage(((Number) metricsData.get("memoryUsage")).doubleValue());
                metrics.setActiveRequests(((Number) metricsData.get("activeRequests")).intValue());
                metrics.setQueueSize(((Number) metricsData.get("queueSize")).longValue());
                metrics.setLastReportTime(((Number) metricsData.get("lastReportTime")).longValue());
                instance.setMetrics(metrics);
            }

            log.debug("Parsed instance: serviceName={}, host={}, port={}, weight={}, healthy={}", serviceName, host, port, weight, healthy);
            return instance;
        } catch (Exception e) {
            log.error("Failed to parse instance JSON: {}", json, e);
            return null;
        }
    }
}