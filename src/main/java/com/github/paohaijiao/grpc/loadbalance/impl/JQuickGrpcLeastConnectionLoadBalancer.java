package com.github.paohaijiao.grpc.loadbalance.impl;

import com.github.paohaijiao.grpc.domain.JQuickGrpcServiceInstance;
import com.github.paohaijiao.grpc.loadbalance.JQuickGrpcLoadBalancer;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 最少连接数负载均衡器
 * 选择当前活跃连接数最少的实例
 * 需要配合指标收集使用
 */
public class JQuickGrpcLeastConnectionLoadBalancer implements JQuickGrpcLoadBalancer {

    // 记录每个实例的活跃连接数
    private final ConcurrentMap<String, AtomicInteger> activeConnections;

    public JQuickGrpcLeastConnectionLoadBalancer() {
        this.activeConnections = new ConcurrentHashMap<>();
    }

    @Override
    public JQuickGrpcServiceInstance select(List<JQuickGrpcServiceInstance> instances) {
        if (instances == null || instances.isEmpty()) {
            return null;
        }

        return instances.stream()
                .filter(JQuickGrpcServiceInstance::isHealthy)
                .min(Comparator.comparingInt(this::getActiveConnections))
                .orElse(null);
    }

    private int getActiveConnections(JQuickGrpcServiceInstance instance) {
        return activeConnections.getOrDefault(instance.getAddress(), new AtomicInteger(0)).get();
    }

    /**
     * 增加连接数（请求开始时调用）
     */
    public void incrementConnection(JQuickGrpcServiceInstance instance) {
        activeConnections.computeIfAbsent(instance.getAddress(), k -> new AtomicInteger(0))
                .incrementAndGet();
        if (instance.getMetrics() != null) {
            instance.getMetrics().setActiveRequests(getActiveConnections(instance));
        }
    }

    /**
     * 减少连接数（请求结束时调用）
     */
    public void decrementConnection(JQuickGrpcServiceInstance instance) {
        AtomicInteger counter = activeConnections.get(instance.getAddress());
        if (counter != null) {
            counter.decrementAndGet();
        }
        if (instance.getMetrics() != null) {
            instance.getMetrics().setActiveRequests(getActiveConnections(instance));
        }
    }

    @Override
    public String getName() {
        return "LeastConnection";
    }
}
