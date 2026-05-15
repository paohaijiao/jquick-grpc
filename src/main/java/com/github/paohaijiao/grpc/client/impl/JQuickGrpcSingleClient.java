package com.github.paohaijiao.grpc.client.impl;
import com.github.paohaijiao.grpc.client.JQuickGrpcClient;
import com.github.paohaijiao.grpc.config.JQuickGrpcClientConfig;
import com.github.paohaijiao.grpc.discovery.JQuickGrpcServiceDiscovery;
import com.github.paohaijiao.grpc.discovery.impl.JQuickGrpcServiceInstance;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * 单连接客户端实现（无连接池）
 * 每个服务实例维护一个长连接，适用于低并发场景
 */
public class JQuickGrpcSingleClient implements JQuickGrpcClient {

    private final JQuickGrpcServiceDiscovery serviceDiscovery;

    private final JQuickGrpcLoadBalancer loadBalancer;

    private final JQuickGrpcClientConfig clientConfig;

    private final Map<String, JQuickGrpcServiceInstance> currentInstanceMap;// 服务名 -> 当前使用的实例

    private final Map<String, ManagedChannel> channelMap;// 服务名 -> Channel 映射

    private final Map<Class<?>, Object> proxyCache;// 代理缓存

    private final Map<String, List<JQuickGrpcServiceInstance>> instanceCache;// 实例缓存（用于快速获取）

    private final AtomicBoolean closed;

    public JQuickGrpcSingleClient(JQuickGrpcClientConfig config, JQuickGrpcServiceDiscovery discovery, JQuickGrpcLoadBalancer loadBalancer) {
        this.clientConfig = config;
        this.serviceDiscovery = discovery;
        this.loadBalancer = loadBalancer;
        this.currentInstanceMap = new ConcurrentHashMap<>();
        this.channelMap = new ConcurrentHashMap<>();
        this.proxyCache = new ConcurrentHashMap<>();
        this.instanceCache = new ConcurrentHashMap<>();
        this.closed = new AtomicBoolean(false);
        if (serviceDiscovery != null) {  // 订阅服务变更
            serviceDiscovery.subscribe("*", (serviceName, instances) -> {
                instanceCache.put(serviceName, instances);
                JQuickGrpcServiceInstance current = currentInstanceMap.get(serviceName);
                if (current != null && !isInstanceAvailable(current, instances)) {// 如果当前使用的实例不再可用，需要重新选择
                    currentInstanceMap.remove(serviceName);
                    closeChannel(serviceName);
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
        return (T) proxyCache.computeIfAbsent(serviceClass, key -> Proxy.newProxyInstance(serviceClass.getClassLoader(), new Class[]{serviceClass}, new SingleGrpcInvocationHandler<>(serviceClass, serviceName)));
    }

    @Override
    public <T> T getServiceWithDeadline(Class<T> serviceClass, String serviceName, long deadlineMillis) {
        T stub = getService(serviceClass, serviceName);
        return withDeadline(stub, deadlineMillis);
    }

    @SuppressWarnings("unchecked")
    private <T> T withDeadline(T stub, long deadlineMillis) {
        if (stub instanceof AbstractStub) {
            return (T) ((AbstractStub<?>) stub).withDeadlineAfter(deadlineMillis, TimeUnit.MILLISECONDS);
        }
        return stub;
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
        instanceCache.clear();
    }

    @Override
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new java.util.HashMap<>();
        stats.put("closed", closed.get());
        stats.put("proxyCacheSize", proxyCache.size());
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
     * 检查实例是否在可用列表中
     */
    private boolean isInstanceAvailable(JQuickGrpcServiceInstance instance, List<JQuickGrpcServiceInstance> instances) {
        if (instance == null || instances == null) {
            return false;
        }
        return instances.stream().filter(JQuickGrpcServiceInstance::isHealthy).anyMatch(i -> i.getAddress().equals(instance.getAddress()));
    }

    /**
     * 获取或创建 Channel
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
     * 关闭指定服务的 Channel
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
     * 获取可用实例列表
     */
    private List<JQuickGrpcServiceInstance> getInstances(String serviceName) {
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

    /**
     * 单连接调用处理器
     */
    private class SingleGrpcInvocationHandler<T> implements InvocationHandler {

        private final Class<T> serviceClass;

        private final String serviceName;

        public SingleGrpcInvocationHandler(Class<T> serviceClass, String serviceName) {
            this.serviceClass = serviceClass;
            this.serviceName = serviceName;
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
            JQuickGrpcServiceInstance instance = getOrSelectInstance();
            if (instance == null) {// 获取服务实例
                throw new RuntimeException("No available service instance for: " + serviceName);
            }
            return invokeWithRetry(method, args, instance);
        }

        /**
         * 获取或选择服务实例
         * 单连接模式下，会尽量复用同一个实例
         */
        private JQuickGrpcServiceInstance getOrSelectInstance() {
            JQuickGrpcServiceInstance cached = currentInstanceMap.get(serviceName);
            if (cached != null && cached.isHealthy()) {// 先检查缓存
                // 验证实例是否还在服务列表中
                List<JQuickGrpcServiceInstance> instances = getInstances(serviceName);
                if (isInstanceAvailable(cached, instances)) {
                    return cached;
                }
            }
            // 重新选择
            List<JQuickGrpcServiceInstance> instances = getInstances(serviceName);
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
                    if (i < maxRetries) {
                        currentInstance.setHealthy(false); // 标记实例不健康
                        List<JQuickGrpcServiceInstance> instances = getInstances(serviceName);// 重新选择实例
                        if (instances != null && !instances.isEmpty()) {
                            currentInstance = loadBalancer.select(instances);
                            if (currentInstance != null) {
                                currentInstanceMap.put(serviceName, currentInstance);
                                Thread.sleep(clientConfig.getRetryDelayMillis());// 重试前等待
                                continue;
                            }
                        }
                    }
                    break;
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
            Method newStubMethod = serviceClass.getMethod("newStub", Channel.class);// 创建 Stub
            Object stub = newStubMethod.invoke(null, interceptedChannel);
            if (clientConfig.getDeadlineMillis() > 0 && stub instanceof AbstractStub) {// 设置 Deadline
                stub = ((AbstractStub<?>) stub).withDeadlineAfter(clientConfig.getDeadlineMillis(), TimeUnit.MILLISECONDS);
            }
            return method.invoke(stub, args);// 调用方法
        }
    }
}
