package com.github.paohaijiao.grpc.client.impl;


import com.github.paohaijiao.grpc.client.JQuickGrpcClient;
import com.github.paohaijiao.grpc.config.JQuickGrpcClientConfig;
import com.github.paohaijiao.grpc.discovery.JQuickGrpcServiceDiscovery;
import com.github.paohaijiao.grpc.domain.JQuickGrpcServiceInstance;
import com.github.paohaijiao.grpc.interceptor.JQuickGrpcClientInterceptor;
import com.github.paohaijiao.grpc.loadbalance.JQuickGrpcLoadBalancer;
import com.github.paohaijiao.grpc.pool.JQuickGrpcChannelPool;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
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

public class JQuickGrpcPooledClient implements JQuickGrpcClient {

    private final JQuickGrpcServiceDiscovery serviceDiscovery;

    private final JQuickGrpcLoadBalancer loadBalancer;

    private final JQuickGrpcClientConfig clientConfig;

    private final Map<String, JQuickGrpcChannelPool> channelPools;

    private final Map<Class<?>, Object> proxyCache;

    private final Map<String, Object> stubCache;

    private final Map<String, List<JQuickGrpcServiceInstance>> instanceCache;

    private final AtomicBoolean closed;

    public JQuickGrpcPooledClient(JQuickGrpcClientConfig config, JQuickGrpcServiceDiscovery discovery, JQuickGrpcLoadBalancer loadBalancer) {
        this.clientConfig = Objects.requireNonNull(config, "clientConfig must not be null");
        this.loadBalancer = Objects.requireNonNull(loadBalancer, "loadBalancer must not be null");
        this.serviceDiscovery = discovery;
        this.channelPools = new ConcurrentHashMap<>();
        this.proxyCache = new ConcurrentHashMap<>();
        this.stubCache = new ConcurrentHashMap<>();
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
        Objects.requireNonNull(serviceClass, "serviceClass must not be null");
        Objects.requireNonNull(serviceName, "serviceName must not be null");
        if (serviceClass.isInterface()) {
            // Legacy path: JDK dynamic proxies support interfaces only (kept for backward compatibility)
            return (T) proxyCache.computeIfAbsent(serviceClass, key -> newProxy(serviceClass, serviceName, 0L));
        }
        // Generated stub classes: bind to the routing channel, rebalance on every RPC
        return (T) stubCache.computeIfAbsent(stubKey(serviceName, serviceClass), key -> createRoutedStub(serviceClass, serviceName));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getServiceWithDeadline(Class<T> serviceClass, String serviceName, long deadlineMillis) {
        T stub = getService(serviceClass, serviceName);
        if (deadlineMillis > 0 && stub instanceof AbstractStub) {
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
            Channel routingChannel = new JQuickGrpcRoutingChannel(serviceName, () -> newPooledTargetChannel(serviceName));
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
     * Invoked on every newCall: selects an instance, borrows a pooled channel and wraps it
     * into a target channel that returns the channel to the pool once the RPC completes.
     */
    private Channel newPooledTargetChannel(String serviceName) {
        JQuickGrpcServiceInstance instance = selectInstance(serviceName);
        if (instance == null) {
            throw new RuntimeException("No available service instance for: " + serviceName);
        }
        String address = instance.getAddress();
        JQuickGrpcChannelPool pool = channelPools.computeIfAbsent(address, k -> new JQuickGrpcChannelPool(address, clientConfig));
        ManagedChannel rawChannel;
        try {
            rawChannel = pool.borrowObject();
        } catch (Exception e) {
            throw new RuntimeException("Failed to borrow channel from pool for: " + address, e);
        }
        return new PooledTargetChannel(rawChannel, pool);
    }

    /**
     * Selects a healthy instance via discovery cache and load balancer.
     */
    private JQuickGrpcServiceInstance selectInstance(String serviceName) {
        List<JQuickGrpcServiceInstance> instances = getHealthyInstances(serviceName);
        if (instances == null || instances.isEmpty()) {
            return null;
        }
        return loadBalancer.select(instances);
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
            instances = instances.stream().filter(JQuickGrpcServiceInstance::isHealthy).collect(java.util.stream.Collectors.toList());
        }
        return instances;
    }

    private static String stubKey(String serviceName, Class<?> stubClass) {
        return serviceName + "#" + stubClass.getName();
    }

    @SuppressWarnings("unchecked")
    private <T> T newProxy(Class<T> serviceClass, String serviceName, long deadlineMillis) {
        return (T) Proxy.newProxyInstance(serviceClass.getClassLoader(), new Class[]{serviceClass}, new GrpcInvocationHandler<>(serviceClass, serviceName, deadlineMillis));
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
        stubCache.clear();
        instanceCache.clear();
    }

    @Override
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new java.util.HashMap<>();
        stats.put("closed", closed.get());
        stats.put("proxyCacheSize", proxyCache.size());
        stats.put("stubCacheSize", stubCache.size());
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

    /**

     * Pooled target channel: wraps the borrowed channel; the ClientCall it creates
     * returns the channel to the pool when the RPC completes (onClose/cancel).
     */
    private static final class PooledTargetChannel extends Channel {

        private final ManagedChannel rawChannel;

        private final JQuickGrpcChannelPool pool;

        PooledTargetChannel(ManagedChannel rawChannel, JQuickGrpcChannelPool pool) {
            this.rawChannel = rawChannel;
            this.pool = pool;
        }

        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(MethodDescriptor<ReqT, RespT> method, CallOptions callOptions) {
            Channel intercepted = ClientInterceptors.intercept(rawChannel, new JQuickGrpcClientInterceptor());
            ClientCall<ReqT, RespT> delegate = intercepted.newCall(method, callOptions);
            return new ReleasingClientCall<>(delegate, pool, rawChannel);
        }

        @Override
        public String authority() {
            return rawChannel.authority();
        }
    }

    /**
     * 在 RPC 结束时归还借出的 Channel。
     * Returns the borrowed channel to the pool when the RPC completes.
     */
    private static final class ReleasingClientCall<ReqT, RespT> extends io.grpc.ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT> {

        private final JQuickGrpcChannelPool pool;

        private final ManagedChannel channel;

        private final AtomicBoolean released;

        ReleasingClientCall(ClientCall<ReqT, RespT> delegate, JQuickGrpcChannelPool pool, ManagedChannel channel) {
            super(delegate);
            this.pool = pool;
            this.channel = channel;
            this.released = new AtomicBoolean(false);
        }

        @Override
        public void start(ClientCall.Listener<RespT> responseListener, Metadata headers) {
            super.start(new io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(responseListener) {
                @Override
                public void onClose(Status status, Metadata trailers) {
                    try {
                        super.onClose(status, trailers);
                    } finally {
                        release();
                    }
                }
            }, headers);
        }

        @Override
        public void cancel(String message, Throwable cause) {
            try {
                super.cancel(message, cause);
            } finally {
                release();
            }
        }

        private void release() {
            if (released.compareAndSet(false, true)) {
                pool.returnObject(channel);
            }
        }
    }

    private class GrpcInvocationHandler<T> implements InvocationHandler {

        private final Class<T> serviceClass;

        private final String serviceName;

        private final long deadlineMillis;

        public GrpcInvocationHandler(Class<T> serviceClass, String serviceName, long deadlineMillis) {
            this.serviceClass = serviceClass;
            this.serviceName = serviceName;
            this.deadlineMillis = deadlineMillis;
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
            return invokeWithRetry(method, args);
        }

        private Object invokeWithRetry(Method method, Object[] args) throws Throwable {
            int maxRetries = clientConfig.getMaxRetries();
            Throwable lastException = null;
            JQuickGrpcServiceInstance instance = null;
            for (int i = 0; i <= maxRetries; i++) {
                try {
                    if (instance == null) {
                        instance = selectInstance(serviceName);
                        if (instance == null) {
                            throw new RuntimeException("No available service instance for: " + serviceName);
                        }
                    }
                    return doInvoke(method, args, instance);
                } catch (Exception e) {
                    lastException = e;
                    if (i >= maxRetries) {
                        break;
                    }
                    Thread.sleep(clientConfig.getRetryDelayMillis());
                    // mark unhealthy and re-select
                    instance.setHealthy(false);
                    instance = selectInstance(serviceName);
                    //  re-selection may return null when no healthy instance remains
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
                Channel interceptedChannel = ClientInterceptors.intercept(rawChannel, new JQuickGrpcClientInterceptor());// 包装拦截器
                Object stub = JQuickGrpcStubFactory.newStub(serviceClass, interceptedChannel);// 创建 stub
                long deadline = deadlineMillis > 0 ? deadlineMillis : clientConfig.getDeadlineMillis();
                if (deadline > 0 && stub instanceof AbstractStub) {// 设置 deadline
                    stub = ((AbstractStub<?>) stub).withDeadlineAfter(deadline, TimeUnit.MILLISECONDS);
                }
                return method.invoke(stub, args);
            } finally {
                pool.returnObject(rawChannel);
            }
        }
    }
}
