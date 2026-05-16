package com.github.paohaijiao.grpc.client.impl;


import com.github.paohaijiao.grpc.client.JQuickGrpcClient;
import com.github.paohaijiao.grpc.config.JQuickGrpcClientConfig;
import com.github.paohaijiao.grpc.discovery.JQuickGrpcServiceDiscovery;
import com.github.paohaijiao.grpc.domain.JQuickGrpcServiceInstance;
import com.github.paohaijiao.grpc.interceptor.JQuickGrpcClientInterceptor;
import com.github.paohaijiao.grpc.loadbalance.JQuickGrpcLoadBalancer;
import com.github.paohaijiao.grpc.pool.JQuickGrpcChannelPool;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.stub.AbstractStub;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class JQuickGrpcPooledClient implements JQuickGrpcClient {

    private final JQuickGrpcServiceDiscovery serviceDiscovery;

    private final JQuickGrpcLoadBalancer loadBalancer;

    private final JQuickGrpcClientConfig clientConfig;

    private final Map<String, JQuickGrpcChannelPool> channelPools;

    private final Map<Class<?>, Object> proxyCache;

    private final Map<String, List<JQuickGrpcServiceInstance>> instanceCache;

    private final AtomicBoolean closed;

    public JQuickGrpcPooledClient(JQuickGrpcClientConfig config, JQuickGrpcServiceDiscovery discovery, JQuickGrpcLoadBalancer loadBalancer) {
        this.clientConfig = config;
        this.serviceDiscovery = discovery;
        this.loadBalancer = loadBalancer;
        this.channelPools = new ConcurrentHashMap<>();
        this.proxyCache = new ConcurrentHashMap<>();
        this.instanceCache = new ConcurrentHashMap<>();
        this.closed = new AtomicBoolean(false);
        if (serviceDiscovery != null) {
            serviceDiscovery.subscribe("*", (serviceName, instances) -> {
                instanceCache.put(serviceName, instances);
            });
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getService(Class<T> serviceClass, String serviceName) {
        if (closed.get()) {
            throw new IllegalStateException("Client already closed");
        }
        return (T) proxyCache.computeIfAbsent(serviceClass, key -> Proxy.newProxyInstance(serviceClass.getClassLoader(), new Class[]{serviceClass}, new GrpcInvocationHandler<>(serviceClass, serviceName))
        );
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
        for (JQuickGrpcChannelPool pool : channelPools.values()) {
            pool.close();
        }
        channelPools.clear();
        proxyCache.clear();
        instanceCache.clear();
    }

    @Override
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new java.util.HashMap<>();
        stats.put("closed", closed.get());
        stats.put("proxyCacheSize", proxyCache.size());
        stats.put("channelPools", channelPools.size());
        return stats;
    }

    @Override
    public String getClientType() {
        return "pooled";
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    private class GrpcInvocationHandler<T> implements InvocationHandler {

        private final Class<T> serviceClass;

        private final String serviceName;

        public GrpcInvocationHandler(Class<T> serviceClass, String serviceName) {
            this.serviceClass = serviceClass;
            this.serviceName = serviceName;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.getName().equals("toString")) {
                return "GrpcProxy[" + serviceName + "]";
            }
            if (method.getName().equals("hashCode")) {
                return hashCode();
            }
            if (method.getName().equals("equals")) {
                return proxy == args[0];
            }
            List<JQuickGrpcServiceInstance> instances = getInstances();
            if (instances == null || instances.isEmpty()) {
                throw new RuntimeException("No available service instance for: " + serviceName);
            }
            JQuickGrpcServiceInstance instance = loadBalancer.select(instances);
            if (instance == null) {
                throw new RuntimeException("Failed to select instance for: " + serviceName);
            }
            return invokeWithRetry(method, args, instance);
        }

        private Object invokeWithRetry(Method method, Object[] args, JQuickGrpcServiceInstance instance) throws Throwable {
            int maxRetries = clientConfig.getMaxRetries();
            Throwable lastException = null;
            for (int i = 0; i <= maxRetries; i++) {
                try {
                    return doInvoke(method, args, instance);
                } catch (Exception e) {
                    lastException = e;
                    if (i < maxRetries) {
                        Thread.sleep(clientConfig.getRetryDelayMillis());
                        // 标记不健康，重新选择
                        instance.setHealthy(false);
                        List<JQuickGrpcServiceInstance> instances = getInstances();
                        if (instances != null && !instances.isEmpty()) {
                            instance = loadBalancer.select(instances);
                        }
                    }
                }
            }
            throw lastException;
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        private Object doInvoke(Method method, Object[] args, JQuickGrpcServiceInstance instance) throws Exception {
            String address = instance.getAddress();
            JQuickGrpcChannelPool pool = channelPools.computeIfAbsent(address, k -> new JQuickGrpcChannelPool(address, clientConfig));
            ManagedChannel rawChannel = pool.borrowObject();
            try {
                Channel interceptedChannel = ClientInterceptors.intercept(rawChannel, new JQuickGrpcClientInterceptor());  // 包装拦截器
                Method newStubMethod = serviceClass.getMethod("newStub", Channel.class);// 创建 stub
                Object stub = newStubMethod.invoke(null, interceptedChannel);
                if (clientConfig.getDeadlineMillis() > 0 && stub instanceof AbstractStub) {// 设置 deadline
                    stub = ((AbstractStub<?>) stub).withDeadlineAfter(clientConfig.getDeadlineMillis(), TimeUnit.MILLISECONDS);
                }
                return method.invoke(stub, args);// 调用方法
            } finally {
                pool.returnObject(rawChannel);
            }
        }

        private List<JQuickGrpcServiceInstance> getInstances() {
            List<JQuickGrpcServiceInstance> instances = instanceCache.get(serviceName);
            if (instances == null && serviceDiscovery != null) {
                instances = serviceDiscovery.getInstances(serviceName);
                if (instances != null && !instances.isEmpty()) {
                    instanceCache.put(serviceName, new java.util.ArrayList<>(instances));
                }
            }
            if (instances != null) {
                instances = instances.stream().filter(JQuickGrpcServiceInstance::isHealthy).collect(java.util.stream.Collectors.toList());
            }
            return instances;
        }
    }
}
