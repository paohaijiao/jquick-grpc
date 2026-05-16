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

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.listener.Event;
import com.alibaba.nacos.api.naming.listener.EventListener;
import com.alibaba.nacos.api.naming.listener.NamingEvent;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.github.paohaijiao.console.JConsole;
import com.github.paohaijiao.grpc.discovery.JQuickGrpcServiceDiscovery;
import com.github.paohaijiao.grpc.domain.JQuickGrpcServiceInstance;
import com.github.paohaijiao.grpc.metadata.JQuickServiceInstanceMetrics;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Nacos 服务发现实现 - 修正版
 * 支持注册多个服务实例
 */
public class JQuickGrpcNacosDiscovery implements JQuickGrpcServiceDiscovery {

    private static final JConsole log = JConsole.getInstance();

    private static final Gson gson = new GsonBuilder().create();

    private NamingService namingService;

    private ConfigService configService;

    private final String serverAddr;

    private final String namespace;

    private final String username;

    private final String password;

    private final String group;

    private final Map<String, List<ServiceChangeListener>> listeners;

    private final Map<String, List<JQuickGrpcServiceInstance>> instanceCache;

    private final Map<String, EventListener> eventListeners;

    private final Map<String, RegisteredInstanceInfo> registeredInstances;

    private final Map<String, ScheduledExecutorService> heartbeatExecutors;

    private final ExecutorService executor;

    private final AtomicBoolean closed;

    private final AtomicBoolean initialized;

    public JQuickGrpcNacosDiscovery(String serverAddr) {
        this(serverAddr, null, null, null, "DEFAULT_GROUP");
    }

    public JQuickGrpcNacosDiscovery(String serverAddr, String username, String password) {
        this(serverAddr, null, username, password, "DEFAULT_GROUP");
    }

    public JQuickGrpcNacosDiscovery(String serverAddr, String namespace, String username, String password, String group) {
        this.serverAddr = serverAddr;
        this.namespace = namespace;
        this.username = username;
        this.password = password;
        this.group = group;
        this.listeners = new ConcurrentHashMap<>();
        this.instanceCache = new ConcurrentHashMap<>();
        this.eventListeners = new ConcurrentHashMap<>();
        this.registeredInstances = new ConcurrentHashMap<>();
        this.heartbeatExecutors = new ConcurrentHashMap<>();
        this.executor = Executors.newCachedThreadPool();
        this.closed = new AtomicBoolean(false);
        this.initialized = new AtomicBoolean(false);
        log.info("Creating Nacos discovery with serverAddr: {}", serverAddr);
        initializeClient();
    }

    private synchronized void initializeClient() {
        if (initialized.get()) {
            return;
        }

        try {
            Properties properties = new Properties();
            properties.setProperty("serverAddr", serverAddr);
            if (namespace != null && !namespace.isEmpty()) {
                properties.setProperty("namespace", namespace);
            }
            if (username != null && !username.isEmpty()) {
                properties.setProperty("username", username);
            }
            if (password != null && !password.isEmpty()) {
                properties.setProperty("password", password);
            }
            this.namingService = NacosFactory.createNamingService(properties);
            this.configService = NacosFactory.createConfigService(properties);
            initialized.set(true);
            log.info("Nacos client initialized successfully");
        } catch (NacosException e) {
            log.error("Failed to initialize Nacos client", e);
            throw new RuntimeException("Failed to initialize Nacos client", e);
        }
    }

    private void ensureInitialized() {
        if (!initialized.get() && !closed.get()) {
            initializeClient();
        }
        if (!initialized.get()) {
            throw new IllegalStateException("Nacos client not initialized");
        }
    }

    @Override
    public List<JQuickGrpcServiceInstance> getInstances(String serviceName) {
        if (closed.get() || !initialized.get()) {
            return Collections.emptyList();
        }
        try {
            ensureInitialized();
            List<Instance> instances = namingService.getAllInstances(serviceName, group);
            List<JQuickGrpcServiceInstance> result = instances.stream()
                    .map(this::convertToJQuickInstance)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            instanceCache.put(serviceName, new ArrayList<>(result));
            log.debug("Discovered {} instances for service: {}", result.size(), serviceName);
            return result;
        } catch (NacosException e) {
            log.error("Failed to get instances from nacos for service: {}", serviceName, e);
            return Collections.emptyList();
        }
    }

    @Override
    public void subscribe(String serviceName, ServiceChangeListener listener) {
        if (closed.get()) {
            return;
        }
        listeners.computeIfAbsent(serviceName, k -> new CopyOnWriteArrayList<>()).add(listener);
        log.info("Subscribed to service changes: {}", serviceName);
        if (!eventListeners.containsKey(serviceName)) {
            createEventListener(serviceName);
        }
        // 立即触发一次
        List<JQuickGrpcServiceInstance> instances = getInstances(serviceName);
        listener.onChange(serviceName, instances);
    }

    @Override
    public void unsubscribe(String serviceName, ServiceChangeListener listener) {
        List<ServiceChangeListener> serviceListeners = listeners.get(serviceName);
        if (serviceListeners != null) {
            serviceListeners.remove(listener);
            if (serviceListeners.isEmpty()) {
                EventListener eventListener = eventListeners.remove(serviceName);
                if (eventListener != null && namingService != null) {
                    try {
                        namingService.unsubscribe(serviceName, group, eventListener);
                        log.info("Unsubscribed from service changes: {}", serviceName);
                    } catch (NacosException e) {
                        log.error("Failed to unsubscribe: {}", serviceName, e);
                    }
                }
                // 清理缓存
                instanceCache.remove(serviceName);
            }
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        log.info("Closing Nacos discovery...");
        for (Map.Entry<String, ScheduledExecutorService> entry : heartbeatExecutors.entrySet()) {
            ScheduledExecutorService heartbeatExecutor = entry.getValue();
            if (heartbeatExecutor != null && !heartbeatExecutor.isShutdown()) {
                heartbeatExecutor.shutdown();
            }
        }
        heartbeatExecutors.clear();
        unregisterAllServices();
        // 关闭所有监听器
        for (Map.Entry<String, EventListener> entry : eventListeners.entrySet()) {
            try {
                namingService.unsubscribe(entry.getKey(), group, entry.getValue());
            } catch (NacosException e) {
                log.warn("Failed to unsubscribe: {}", entry.getKey(), e);
            }
        }
        eventListeners.clear();
        // 关闭客户端
        if (namingService != null) {
            try {
                namingService.shutDown();
            } catch (NacosException e) {
                log.warn("Error shutting down naming service", e);
            }
        }
        executor.shutdown();
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        initialized.set(false);
        log.info("Nacos discovery closed");
    }

    private void createEventListener(String serviceName) {
        if (closed.get() || !initialized.get()) {
            return;
        }
        try {
            ensureInitialized();
            EventListener eventListener = new EventListener() {
                @Override
                public void onEvent(Event event) {
                    if (event instanceof NamingEvent) {
                        log.debug("Service change event for: {}", serviceName);
                        instanceCache.remove(serviceName);
                        handleServiceChange(serviceName);
                    }
                }
            };
            namingService.subscribe(serviceName, group, eventListener);
            eventListeners.put(serviceName, eventListener);
            log.info("EventListener created for service: {}", serviceName);
        } catch (NacosException e) {
            log.error("Failed to create event listener for service: {}", serviceName, e);
        }
    }

    private void handleServiceChange(String serviceName) {
        List<JQuickGrpcServiceInstance> newInstances = getInstances(serviceName);
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
     * 注册服务实例（支持多个）
     */
    public void registerService(String serviceName, String host, int port) {
        registerService(serviceName, host, port, 1, null);
    }

    public void registerService(String serviceName, String host, int port, int weight) {
        registerService(serviceName, host, port, weight, null);
    }

    /**
     * 注册服务实例（支持多个）
     */
    public void registerService(String serviceName, String host, int port, int weight, JQuickServiceInstanceMetrics metrics) {
        String instanceKey = generateInstanceKey(serviceName, host, port);
        if (registeredInstances.containsKey(instanceKey)) {
            log.warn("Service instance already registered: {}", instanceKey);
            return;
        }
        try {
            ensureInitialized();
            Instance instance = createNacosInstance(serviceName, host, port, weight, metrics);
            namingService.registerInstance(serviceName, group, instance);
            // 保存注册信息
            RegisteredInstanceInfo info = new RegisteredInstanceInfo();
            info.serviceName = serviceName;
            info.host = host;
            info.port = port;
            info.weight = weight;
            info.metrics = metrics;
            registeredInstances.put(instanceKey, info);
            startHeartbeat(instanceKey, serviceName, host, port, weight, metrics);
            log.info("Registered service instance: {} at {}:{} (weight={})", serviceName, host, port, weight);
            instanceCache.remove(serviceName);
        } catch (NacosException e) {
            log.error("Failed to register service: {}", serviceName, e);
            throw new RuntimeException("Failed to register service", e);
        }
    }

    private Instance createNacosInstance(String serviceName, String host, int port, int weight, JQuickServiceInstanceMetrics metrics) {
        Instance instance = new Instance();
        instance.setIp(host);
        instance.setPort(port);
        instance.setWeight(weight);
        instance.setEnabled(true);
        instance.setHealthy(true);
        instance.setServiceName(serviceName);
        Map<String, String> metadata = new HashMap<>();
        metadata.put("serviceName", serviceName);
        metadata.put("weight", String.valueOf(weight));
        if (metrics != null) {
            metadata.put("metrics", gson.toJson(metrics));
        }
        instance.setMetadata(metadata);
        return instance;
    }

    /**
     * 启动心跳 - 通过定时重新注册来保持心跳
     * Nacos 会通过客户端的心跳机制自动维护健康状态
     */
    private void startHeartbeat(String instanceKey, String serviceName, String host, int port, int weight, JQuickServiceInstanceMetrics metrics) {
        ScheduledExecutorService heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
        heartbeatExecutors.put(instanceKey, heartbeatExecutor);
        heartbeatExecutor.scheduleAtFixedRate(() -> {
            try {
                if (!closed.get() && initialized.get() && namingService != null) {
                    Instance instance = createNacosInstance(serviceName, host, port, weight, metrics);
                    namingService.registerInstance(serviceName, group, instance);
                    log.debug("Heartbeat sent for instance: {}", instanceKey);
                }
            } catch (Exception e) {
                log.warn("Failed to send heartbeat for instance: {}", instanceKey, e);
            }
        }, 10, 10, TimeUnit.SECONDS);  // 每10秒重新注册一次
    }

    /**
     * 更新服务实例的 Metrics
     */
    public void updateMetrics(String serviceName, String host, int port, JQuickServiceInstanceMetrics metrics) {
        String instanceKey = generateInstanceKey(serviceName, host, port);
        RegisteredInstanceInfo info = registeredInstances.get(instanceKey);
        if (info == null) {
            log.warn("Cannot update metrics, instance not registered: {}", instanceKey);
            return;
        }

        info.metrics = metrics;
        log.debug("Updated metrics for instance: {}", instanceKey);
        try {
            Instance instance = createNacosInstance(serviceName, host, port, info.weight, metrics);
            namingService.registerInstance(serviceName, group, instance);
            instanceCache.remove(serviceName);
        } catch (NacosException e) {
            log.error("Failed to update metrics", e);
        }
    }

    /**
     * 注销服务实例
     */
    public void unregisterService(String serviceName, String host, int port) {
        String instanceKey = generateInstanceKey(serviceName, host, port);
        RegisteredInstanceInfo info = registeredInstances.remove(instanceKey);
        if (info == null) {
            log.warn("Instance not registered: {}", instanceKey);
            return;
        }
        try {
            if (initialized.get() && namingService != null) {
                namingService.deregisterInstance(serviceName, group, host, port);
            }
            ScheduledExecutorService heartbeatExecutor = heartbeatExecutors.remove(instanceKey);
            if (heartbeatExecutor != null && !heartbeatExecutor.isShutdown()) {
                heartbeatExecutor.shutdown();
            }
            log.info("Unregistered service instance: {}", instanceKey);
            instanceCache.remove(serviceName);

        } catch (NacosException e) {
            log.error("Failed to unregister service instance: {}", instanceKey, e);
        }
    }

    /**
     * 注销所有服务
     */
    public void unregisterAllServices() {
        for (String instanceKey : new ArrayList<>(registeredInstances.keySet())) {
            RegisteredInstanceInfo info = registeredInstances.get(instanceKey);
            if (info != null) {
                unregisterService(info.serviceName, info.host, info.port);
            }
        }
    }

    /**
     * 更新服务实例的健康状态
     */
    public void updateHealth(String serviceName, String host, int port, boolean healthy) {
        String instanceKey = generateInstanceKey(serviceName, host, port);
        RegisteredInstanceInfo info = registeredInstances.get(instanceKey);
        if (info == null) {
            log.warn("Cannot update health, instance not registered: {}", instanceKey);
            return;
        }

        try {
            Instance instance = createNacosInstance(serviceName, host, port, info.weight, info.metrics);
            instance.setHealthy(healthy);
            instance.setEnabled(healthy);
            namingService.registerInstance(serviceName, group, instance);
            log.info("Updated health status for instance: {} to {}", instanceKey, healthy);
            instanceCache.remove(serviceName);

        } catch (NacosException e) {
            log.error("Failed to update health status", e);
        }
    }

    /**
     * 获取当前注册的所有实例（用于调试）
     */
    public Map<String, RegisteredInstanceInfo> getRegisteredInstances() {
        return new HashMap<>(registeredInstances);
    }

    private String generateInstanceKey(String serviceName, String host, int port) {
        return serviceName + "|" + host + ":" + port;
    }

    private JQuickGrpcServiceInstance convertToJQuickInstance(Instance instance) {
        try {
            String serviceName = instance.getServiceName();
            if (serviceName != null && serviceName.contains("@@")) {
                serviceName = serviceName.substring(serviceName.lastIndexOf("@@") + 2);
            }
            JQuickGrpcServiceInstance jqInstance = new JQuickGrpcServiceInstance(serviceName, instance.getIp(), instance.getPort());
            jqInstance.setWeight((int) instance.getWeight());
            jqInstance.setHealthy(instance.isHealthy() && instance.isEnabled());
            Map<String, String> metadata = instance.getMetadata();
            if (metadata != null && metadata.containsKey("metrics")) {
                String metricsJson = metadata.get("metrics");
                JQuickServiceInstanceMetrics metrics = gson.fromJson(metricsJson, JQuickServiceInstanceMetrics.class);
                jqInstance.setMetrics(metrics);
            }
            return jqInstance;
        } catch (Exception e) {
            log.error("Failed to convert instance", e);
            return null;
        }
    }

    public static class RegisteredInstanceInfo {
        public String serviceName;
        public String host;
        public int port;
        public int weight;
        public JQuickServiceInstanceMetrics metrics;
    }
}