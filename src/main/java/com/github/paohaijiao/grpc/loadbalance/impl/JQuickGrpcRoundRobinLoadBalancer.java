package com.github.paohaijiao.grpc.loadbalance.impl;

import com.github.paohaijiao.grpc.domain.JQuickGrpcServiceInstance;
import com.github.paohaijiao.grpc.loadbalance.JQuickGrpcLoadBalancer;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class JQuickGrpcRoundRobinLoadBalancer implements JQuickGrpcLoadBalancer {

    private final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public JQuickGrpcServiceInstance select(List<JQuickGrpcServiceInstance> instances) {
        if (instances == null || instances.isEmpty()) {
            return null;
        }
        int index = Math.abs(counter.getAndIncrement() % instances.size());
        return instances.get(index);
    }

    @Override
    public String getName() {
        return "RoundRobin";
    }
}
