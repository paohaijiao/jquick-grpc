package com.github.paohaijiao.grpc.loadbalance.impl;

import com.github.paohaijiao.grpc.discovery.impl.JQuickGrpcServiceInstance;
import com.github.paohaijiao.grpc.loadbalance.JQuickGrpcLoadBalancer;

import java.security.SecureRandom;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 加权负载均衡器
 * 根据服务实例的权重进行选择，权重越高被选中的概率越大
 * <p>
 * 支持两种算法：
 * 1. 随机加权：根据权重随机选择
 * 2. 轮询加权：平滑加权轮询算法
 */
public class JQuickGrpcWeightedLoadBalancer implements JQuickGrpcLoadBalancer {

    private final Random random;
    private final Algorithm algorithm;
    private final ConcurrentMap<String, SmoothWeightedRoundRobinState> stateMap;

    public JQuickGrpcWeightedLoadBalancer() {
        this(Algorithm.SMOOTH_RR);
    }

    public JQuickGrpcWeightedLoadBalancer(Algorithm algorithm) {
        this.random = new SecureRandom();
        this.algorithm = algorithm;
        this.stateMap = new ConcurrentHashMap<>();
    }

    @Override
    public JQuickGrpcServiceInstance select(List<JQuickGrpcServiceInstance> instances) {
        if (instances == null || instances.isEmpty()) {
            return null;
        }
        if (instances.size() == 1) {
            return instances.get(0);
        }

        // 检查是否所有权重相同
        boolean allSameWeight = checkAllSameWeight(instances);
        if (allSameWeight) {
            // 如果权重相同，退化为随机选择
            return instances.get(random.nextInt(instances.size()));
        }

        switch (algorithm) {
            case RANDOM:
                return selectByRandomWeight(instances);
            case SMOOTH_RR:
                return selectBySmoothWeightedRoundRobin(instances);
            default:
                return selectByRandomWeight(instances);
        }
    }

    @Override
    public String getName() {
        return "Weighted(" + algorithm + ")";
    }

    /**
     * 随机加权算法
     * 根据权重比例随机选择
     */
    private JQuickGrpcServiceInstance selectByRandomWeight(List<JQuickGrpcServiceInstance> instances) {
        int totalWeight = instances.stream()
                .filter(JQuickGrpcServiceInstance::isHealthy)
                .mapToInt(JQuickGrpcServiceInstance::getWeight)
                .sum();

        if (totalWeight <= 0) {
            return instances.get(0);
        }

        int randomWeight = random.nextInt(totalWeight);
        int currentWeight = 0;

        for (JQuickGrpcServiceInstance instance : instances) {
            if (!instance.isHealthy()) {
                continue;
            }
            currentWeight += instance.getWeight();
            if (randomWeight < currentWeight) {
                return instance;
            }
        }

        return instances.get(0);
    }

    /**
     * 平滑加权轮询算法（Nginx 使用的算法）
     * 可以避免权重高的实例被连续选中
     */
    private JQuickGrpcServiceInstance selectBySmoothWeightedRoundRobin(List<JQuickGrpcServiceInstance> instances) {
        // 生成服务列表的唯一标识（用于缓存状态）
        String instancesKey = generateInstancesKey(instances);

        SmoothWeightedRoundRobinState state = stateMap.computeIfAbsent(instancesKey,
                k -> new SmoothWeightedRoundRobinState());

        synchronized (state) {
            // 检查实例列表是否发生变化
            if (state.instanceCount != instances.size()) {
                state.reset();
                state.instanceCount = instances.size();
            }

            int totalWeight = 0;
            JQuickGrpcServiceInstance selected = null;
            int maxCurrentWeight = -1;

            for (int i = 0; i < instances.size(); i++) {
                JQuickGrpcServiceInstance instance = instances.get(i);
                if (!instance.isHealthy()) {
                    continue;
                }

                int weight = instance.getWeight();
                totalWeight += weight;

                // 更新 currentWeight
                int currentWeight = state.currentWeights.getOrDefault(i, 0) + weight;
                state.currentWeights.put(i, currentWeight);

                // 选择 currentWeight 最大的实例
                if (currentWeight > maxCurrentWeight) {
                    maxCurrentWeight = currentWeight;
                    selected = instance;
                    state.selectedIndex = i;
                }
            }

            if (selected == null) {
                return instances.get(0);
            }

            // 更新被选中实例的 currentWeight
            state.currentWeights.put(state.selectedIndex,
                    state.currentWeights.get(state.selectedIndex) - totalWeight);

            return selected;
        }
    }

    /**
     * 检查是否所有权重相同
     */
    private boolean checkAllSameWeight(List<JQuickGrpcServiceInstance> instances) {
        if (instances.isEmpty()) {
            return true;
        }
        int firstWeight = instances.get(0).getWeight();
        for (JQuickGrpcServiceInstance instance : instances) {
            if (instance.getWeight() != firstWeight) {
                return false;
            }
        }
        return true;
    }

    /**
     * 生成实例列表的唯一标识
     */
    private String generateInstancesKey(List<JQuickGrpcServiceInstance> instances) {
        StringBuilder sb = new StringBuilder();
        for (JQuickGrpcServiceInstance instance : instances) {
            sb.append(instance.getAddress()).append(":").append(instance.getWeight()).append(",");
        }
        return sb.toString();
    }

    public enum Algorithm {
        RANDOM,      // 随机加权
        SMOOTH_RR    // 平滑加权轮询
    }

    /**
     * 平滑加权轮询状态
     */
    private static class SmoothWeightedRoundRobinState {
        final ConcurrentMap<Integer, Integer> currentWeights = new ConcurrentHashMap<>();
        int instanceCount;
        int selectedIndex;

        void reset() {
            currentWeights.clear();
            selectedIndex = -1;
        }
    }
}
