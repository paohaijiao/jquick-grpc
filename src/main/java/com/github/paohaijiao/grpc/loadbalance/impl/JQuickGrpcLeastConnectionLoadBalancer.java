package com.github.paohaijiao.grpc.loadbalance.impl;

import com.github.paohaijiao.grpc.domain.JQuickGrpcServiceInstance;
import com.github.paohaijiao.grpc.loadbalance.JQuickGrpcLoadBalancer;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Least‑connections load balancer
 * Selects the instance with minimal active connections
 * Requires metrics collection
 */
public class JQuickGrpcLeastConnectionLoadBalancer implements JQuickGrpcLoadBalancer {

    // record active connections for each address
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
     * increment connection count for the instance
     */
    public void incrementConnection(JQuickGrpcServiceInstance instance) {
        activeConnections.computeIfAbsent(instance.getAddress(), k -> new AtomicInteger(0))
                .incrementAndGet();
        if (instance.getMetrics() != null) {
            instance.getMetrics().setActiveRequests(getActiveConnections(instance));
        }
    }

    /**
     * reduce connection count for the instance
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
