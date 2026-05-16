package com.github.paohaijiao.grpc.loadbalance.impl;

import com.github.paohaijiao.grpc.domain.JQuickGrpcServiceInstance;
import com.github.paohaijiao.grpc.loadbalance.JQuickGrpcLoadBalancer;

import java.security.SecureRandom;
import java.util.List;
import java.util.Random;

/**
 * 随机负载均衡器
 * 从可用实例中随机选择一个
 */
public class JQuickGrpcRandomLoadBalancer implements JQuickGrpcLoadBalancer {

    private final Random random;

    public JQuickGrpcRandomLoadBalancer() {
        this.random = new SecureRandom();
    }

    public JQuickGrpcRandomLoadBalancer(Random random) {
        this.random = random;
    }

    @Override
    public JQuickGrpcServiceInstance select(List<JQuickGrpcServiceInstance> instances) {
        if (instances == null || instances.isEmpty()) {
            return null;
        }
        if (instances.size() == 1) {
            return instances.get(0);
        }
        int index = random.nextInt(instances.size());
        return instances.get(index);
    }

    @Override
    public String getName() {
        return "Random";
    }
}
