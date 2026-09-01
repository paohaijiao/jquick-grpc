package com.github.paohaijiao.grpc.loadbalance.impl;

import com.github.paohaijiao.grpc.domain.JQuickGrpcServiceInstance;
import com.github.paohaijiao.grpc.loadbalance.JQuickGrpcLoadBalancer;

import java.security.SecureRandom;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * Weighted Load Balancer
 * Selects service instances based on weight; higher weight means higher probability of being selected
 * <p>
 * Two algorithms are supported:
 * 1. Weighted Random: Random selection according to weight
 * 2. Weighted Round‑Robin: Smooth weighted round‑robin algorithm
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
        //  filter unhealthy instances first so the equal-weight fast path cannot pick a dead node
        List<JQuickGrpcServiceInstance> healthy = instances.stream()
                .filter(JQuickGrpcServiceInstance::isHealthy)
                .collect(Collectors.toList());
        if (healthy.isEmpty()) {
            return instances.get(0);//  fall back to the first instance when none is healthy
        }
        if (healthy.size() == 1) {
            return healthy.get(0);
        }
        boolean allSameWeight = checkAllSameWeight(healthy);
        if (allSameWeight) {
            return healthy.get(random.nextInt(healthy.size()));
        }

        switch (algorithm) {
            case RANDOM:
                return selectByRandomWeight(healthy);
            case SMOOTH_RR:
                return selectBySmoothWeightedRoundRobin(healthy);
            default:
                return selectByRandomWeight(healthy);
        }
    }

    @Override
    public String getName() {
        return "Weighted(" + algorithm + ")";
    }

    /**
     * Weighted Random Algorithm
     * Performs random selection according to weight proportions
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
     * Smooth weighted round‑robin algorithm (adopted by Nginx)
     * Avoids consecutive selections of high‑weight instances
     */
    private JQuickGrpcServiceInstance selectBySmoothWeightedRoundRobin(List<JQuickGrpcServiceInstance> instances) {
        String instancesKey = generateInstancesKey(instances);
        SmoothWeightedRoundRobinState state = stateMap.computeIfAbsent(instancesKey, k -> new SmoothWeightedRoundRobinState());
        synchronized (state) {
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
                int currentWeight = state.currentWeights.getOrDefault(i, 0) + weight;
                state.currentWeights.put(i, currentWeight);
                if (currentWeight > maxCurrentWeight) {// 选择 currentWeight 最大的实例
                    maxCurrentWeight = currentWeight;
                    selected = instance;
                    state.selectedIndex = i;
                }
            }
            if (selected == null) {
                return instances.get(0);
            }
            state.currentWeights.put(state.selectedIndex, state.currentWeights.get(state.selectedIndex) - totalWeight);// 更新被选中实例的 currentWeight
            return selected;
        }
    }

    /**
     * Check if all weights are the same
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
     * Generates unique identifier for instance list
     */
    private String generateInstancesKey(List<JQuickGrpcServiceInstance> instances) {
        StringBuilder sb = new StringBuilder();
        for (JQuickGrpcServiceInstance instance : instances) {
            // Include health in the key so smooth-RR state is rebuilt when instances go up/down
            sb.append(instance.getAddress()).append(":").append(instance.getWeight())
                    .append(":").append(instance.isHealthy()).append(",");
        }
        return sb.toString();
    }

    public enum Algorithm {
        RANDOM,
        SMOOTH_RR
    }

    /**
     * Smooth weighted round‑robin state
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
