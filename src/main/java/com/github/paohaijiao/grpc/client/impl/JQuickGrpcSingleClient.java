package com.github.paohaijiao.grpc.client.impl;
import com.github.paohaijiao.grpc.client.JQuickGrpcClient;
import com.github.paohaijiao.grpc.config.JQuickGrpcClientConfig;
import com.github.paohaijiao.grpc.discovery.JQuickGrpcServiceDiscovery;
import com.github.paohaijiao.grpc.domain.JQuickGrpcServiceInstance;
import com.github.paohaijiao.grpc.interceptor.JQuickGrpcClientInterceptor;
import com.github.paohaijiao.grpc.loadbalance.JQuickGrpcLoadBalancer;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.AbstractStub;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;


public class JQuickGrpcSingleClient implements JQuickGrpcClient {

    private final JQuickGrpcServiceDiscovery serviceDiscovery;

    private final JQuickGrpcLoadBalancer loadBalancer;

    private final JQuickGrpcClientConfig clientConfig;

    private final Map<String, JQuickGrpcServiceInstance> currentInstanceMap;

    private final Map<String, ManagedChannel> channelMap;

    private final Map<Class<?>, Object> proxyCache;

    private final Map<String, Object> stubCache;

    private final Map<String, List<JQuickGrpcServiceInstance>> instanceCache;

    private final AtomicBoolean closed;

    public JQuickGrpcSingleClient(JQuickGrpcClientConfig config, JQuickGrpcServiceDiscovery discovery, JQuickGrpcLoadBalancer loadBalancer) {
        this.clientConfig = Objects.requireNonNull(config, "clientConfig must not be null");
        this.loadBalancer = Objects.requireNonNull(loadBalancer, "loadBalancer must not be null");
        this.serviceDiscovery = discovery;
        this.currentInstanceMap = new ConcurrentHashMap<>();
        this.channelMap = new ConcurrentHashMap<>();
        this.proxyCache = new ConcurrentHashMap<>();
        this.stubCache = new ConcurrentHashMap<>();
        this.instanceCache = new ConcurrentHashMap<>();
        this.closed = new AtomicBoolean(false);
        if (serviceDiscovery != null) {  // 订阅服务变更
            serviceDiscovery.subscribe("*", (serviceName, instances) -> {
                instanceCache.put(serviceName, instances);
                JQuickGrpcServiceInstance current = currentInstanceMap.get(serviceName);
                if (current != null && !isInstanceAvailable(current, instances)) {
                    //  close the stale channel BEFORE removing the mapping, otherwise closeChannel
                    // cannot resolve the instance and the channel leaks
                    closeChannel(serviceName);
                    currentInstanceMap.remove(serviceName);
                }
            });
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getService(Class<T> serviceClass, String serviceName) {
        if (closed.get()) {
            throw new IllegalStateException("Client already closed");
        }
        Objects.requireNonNull(serviceClass, "serviceClass must not be null");
        Objects.requireNonNull(serviceName, "serviceName must not be null");
        if (serviceClass.isInterface()) {
            // Legacy path: JDK dynamic proxies support interfaces only (kept for backward compatibility)
            return (T) proxyCache.computeIfAbsent(serviceClass, key -> newProxy(serviceClass, serviceName, 0L));
        }
        // Generated stub classes: bind to the routing channel, re-select instance on every RPC
        return (T) stubCache.computeIfAbsent(stubKey(serviceName, serviceClass), key -> createRoutedStub(serviceClass, serviceName));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getServiceWithDeadline(Class<T> serviceClass, String serviceName, long deadlineMillis) {
        T stub = getService(serviceClass, serviceName);
        if (deadlineMillis > 0 && stub instanceof AbstractStub) {
            // real stubs are AbstractStub subclasses
            return (T) ((AbstractStub<?>) stub).withDeadlineAfter(deadlineMillis, TimeUnit.MILLISECONDS);
        }
        return stub;
    }

    /**
     * Creates a real stub bound to the routing channel (package-private for testing).
     */
    @SuppressWarnings("unchecked")
    <T> T createRoutedStub(Class<T> stubClass, String serviceName) {
        try {
            Channel routingChannel = new JQuickGrpcRoutingChannel(serviceName, () -> newSingleTargetChannel(serviceName));
            Object stub = JQuickGrpcStubFactory.newStub(stubClass, routingChannel);
            long deadline = clientConfig.getDeadlineMillis();
            if (deadline > 0 && stub instanceof AbstractStub) {
                stub = ((AbstractStub<?>) stub).withDeadlineAfter(deadline, TimeUnit.MILLISECONDS);
            }
            return (T) stub;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create gRPC stub for: " + stubClass.getName(), e);
        }
    }

    /**
     * Invoked on every newCall: selects an instance and returns the intercepted channel.
     */
    private Channel newSingleTargetChannel(String serviceName) {
        JQuickGrpcServiceInstance instance = selectInstance(serviceName);
        if (instance == null) {
            throw new RuntimeException("No available service instance for: " + serviceName);
        }
        ManagedChannel rawChannel = getOrCreateChannel(instance);
        return ClientInterceptors.intercept(rawChannel, new JQuickGrpcClientInterceptor());
    }

    /**
     * Selects the current instance (single mode reuses the same instance when possible).
     */
    private JQuickGrpcServiceInstance selectInstance(String serviceName) {
        JQuickGrpcServiceInstance cached = currentInstanceMap.get(serviceName);
        if (cached != null && cached.isHealthy()) {
            List<JQuickGrpcServiceInstance> instances = getHealthyInstances(serviceName);
            if (isInstanceAvailable(cached, instances)) {
                return cached;
            }
        }
        List<JQuickGrpcServiceInstance> instances = getHealthyInstances(serviceName);
        if (instances == null || instances.isEmpty()) {
            return null;
        }
        JQuickGrpcServiceInstance selected = loadBalancer.select(instances);
        if (selected != null) {
            currentInstanceMap.put(serviceName, selected);
        }
        return selected;
    }

    /**
     * Returns healthy instances (cache first, falls back to discovery).
     */
    private List<JQuickGrpcServiceInstance> getHealthyInstances(String serviceName) {
        List<JQuickGrpcServiceInstance> instances = instanceCache.get(serviceName);
        if (instances == null && serviceDiscovery != null) {
            instances = serviceDiscovery.getInstances(serviceName);
            if (instances != null && !instances.isEmpty()) {
                instanceCache.put(serviceName, new java.util.ArrayList<>(instances));
            }
        }
        if (instances != null) {
            instances = instances.stream().filter(JQuickGrpcServiceInstance::isHealthy).collect(Collectors.toList());
        }
        return instances;
    }

    private static String stubKey(String serviceName, Class<?> stubClass) {
        return serviceName + "#" + stubClass.getName();
    }

    @SuppressWarnings("unchecked")
    private <T> T newProxy(Class<T> serviceClass, String serviceName, long deadlineMillis) {
        return (T) Proxy.newProxyInstance(serviceClass.getClassLoader(), new Class[]{serviceClass}, new SingleGrpcInvocationHandler<>(serviceClass, serviceName, deadlineMillis));
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        if (serviceDiscovery != null) {
            serviceDiscovery.close();
        }
        for (Map.Entry<String, ManagedChannel> entry : channelMap.entrySet()) {// 关闭所有 Channel
            ManagedChannel channel = entry.getValue();
            if (channel != null && !channel.isShutdown()) {
                channel.shutdown();
                try {
                    channel.awaitTermination(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    channel.shutdownNow();
                }
            }
        }
        channelMap.clear();
        currentInstanceMap.clear();
        proxyCache.clear();
        stubCache.clear();
        instanceCache.clear();
    }

    @Override
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new java.util.HashMap<>();
        stats.put("closed", closed.get());
        stats.put("proxyCacheSize", proxyCache.size());
        stats.put("stubCacheSize", stubCache.size());
        stats.put("channelCount", channelMap.size());
        stats.put("currentInstances", currentInstanceMap.size());
        Map<String, String> channelStatus = new java.util.HashMap<>();
        for (Map.Entry<String, ManagedChannel> entry : channelMap.entrySet()) {// 各 Channel 状态
            ManagedChannel ch = entry.getValue();
            channelStatus.put(entry.getKey(), "state=" + ch.getState(false) + ", isShutdown=" + ch.isShutdown() + ", isTerminated=" + ch.isTerminated());
        }
        stats.put("channelStatus", channelStatus);
        return stats;
    }

    @Override
    public String getClientType() {
        return "single";
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    /**
     * Checks if the instance is available in the list of healthy instances.
     */
    private boolean isInstanceAvailable(JQuickGrpcServiceInstance instance, List<JQuickGrpcServiceInstance> instances) {
        if (instance == null || instances == null) {
            return false;
        }
        return instances.stream().filter(JQuickGrpcServiceInstance::isHealthy).anyMatch(i -> i.getAddress().equals(instance.getAddress()));
    }

    /**
     * get or create Channel
     */
    private ManagedChannel getOrCreateChannel(JQuickGrpcServiceInstance instance) {
        return channelMap.computeIfAbsent(instance.getAddress(), k -> {
            NettyChannelBuilder builder = NettyChannelBuilder.forAddress(instance.getHost(), instance.getPort());
            if (clientConfig.isUsePlaintext()) {
                builder.usePlaintext();
            }
            builder.maxInboundMessageSize(clientConfig.getMaxInboundMessageSize())
                    .keepAliveTime(clientConfig.getKeepAliveTimeSeconds(), TimeUnit.SECONDS)
                    .keepAliveWithoutCalls(clientConfig.isKeepAliveWithoutCalls());
            return builder.build();
        });
    }

    /**
     * close the channel for the specified service
     */
    private void closeChannel(String serviceName) {
        JQuickGrpcServiceInstance instance = currentInstanceMap.get(serviceName);
        if (instance != null) {
            ManagedChannel channel = channelMap.remove(instance.getAddress());
            if (channel != null && !channel.isShutdown()) {
                channel.shutdown();
            }
        }
    }

    /**
     *  keep the original behavior of the interface
     */
    private class SingleGrpcInvocationHandler<T> implements InvocationHandler {

        private final Class<T> serviceClass;

        private final String serviceName;

        private final long deadlineMillis;

        public SingleGrpcInvocationHandler(Class<T> serviceClass, String serviceName, long deadlineMillis) {
            this.serviceClass = serviceClass;
            this.serviceName = serviceName;
            this.deadlineMillis = deadlineMillis;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.getName().equals("toString")) {
                return "GrpcSingleProxy[" + serviceName + "]";
            }
            if (method.getName().equals("hashCode")) {
                return hashCode();
            }
            if (method.getName().equals("equals")) {
                return proxy == args[0];
            }
            JQuickGrpcServiceInstance instance = selectInstance(serviceName);// 获取服务实例
            if (instance == null) {
                throw new RuntimeException("No available service instance for: " + serviceName);
            }
            return invokeWithRetry(method, args, instance);
        }

        /**
         * 带重试的调用
         */
        private Object invokeWithRetry(Method method, Object[] args, JQuickGrpcServiceInstance instance) throws Throwable {
            int maxRetries = clientConfig.getMaxRetries();
            Throwable lastException = null;
            JQuickGrpcServiceInstance currentInstance = instance;
            for (int i = 0; i <= maxRetries; i++) {
                try {
                    return doInvoke(method, args, currentInstance);
                } catch (Exception e) {
                    lastException = e;
                    if (i >= maxRetries) {
                        break;
                    }
                    currentInstance.setHealthy(false);
                    List<JQuickGrpcServiceInstance> instances = getHealthyInstances(serviceName);// 重新选择实例
                    JQuickGrpcServiceInstance next = (instances != null && !instances.isEmpty()) ? loadBalancer.select(instances) : null;
                    if (next == null) {
                        //  break instead of NPE when no healthy instance remains
                        break;
                    }
                    currentInstance = next;
                    currentInstanceMap.put(serviceName, currentInstance);
                    Thread.sleep(clientConfig.getRetryDelayMillis());
                }
            }
            throw lastException;
        }

        /**
         * 实际调用
         */
        @SuppressWarnings({"unchecked", "rawtypes"})
        private Object doInvoke(Method method, Object[] args, JQuickGrpcServiceInstance instance) throws Exception {
            ManagedChannel rawChannel = getOrCreateChannel(instance);   // 获取 Channel
            Channel interceptedChannel = ClientInterceptors.intercept(rawChannel, new JQuickGrpcClientInterceptor());// 包装拦截器
            Object stub = JQuickGrpcStubFactory.newStub(serviceClass, interceptedChannel);// 创建 Stub
            long deadline = deadlineMillis > 0 ? deadlineMillis : clientConfig.getDeadlineMillis();
            if (deadline > 0 && stub instanceof AbstractStub) {// 设置 Deadline
                stub = ((AbstractStub<?>) stub).withDeadlineAfter(deadline, TimeUnit.MILLISECONDS);
            }
            return method.invoke(stub, args);// 调用方法
        }
    }
}
