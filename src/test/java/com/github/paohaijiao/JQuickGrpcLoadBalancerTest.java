package com.github.paohaijiao;


import com.github.paohaijiao.grpc.domain.JQuickGrpcServiceInstance;
import com.github.paohaijiao.grpc.loadbalance.impl.JQuickGrpcLeastConnectionLoadBalancer;
import com.github.paohaijiao.grpc.loadbalance.impl.JQuickGrpcRandomLoadBalancer;
import com.github.paohaijiao.grpc.loadbalance.impl.JQuickGrpcRoundRobinLoadBalancer;
import com.github.paohaijiao.grpc.loadbalance.impl.JQuickGrpcWeightedLoadBalancer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;

class JQuickGrpcLoadBalancerTest {

    private List<JQuickGrpcServiceInstance> instances;

    @BeforeEach
    void setUp() {
        instances = new ArrayList<>();
        instances.add(new JQuickGrpcServiceInstance("test", "localhost", 8080));
        instances.add(new JQuickGrpcServiceInstance("test", "localhost", 8081));
        instances.add(new JQuickGrpcServiceInstance("test", "localhost", 8082));
    }

    @Test
    void testRandomLoadBalancer() {
        JQuickGrpcRandomLoadBalancer balancer = new JQuickGrpcRandomLoadBalancer();
        JQuickGrpcServiceInstance selected = balancer.select(instances);
        assertNotNull(selected);
        assertTrue(instances.contains(selected));
        assertEquals("Random", balancer.getName());
    }

    @Test
    void testRandomLoadBalancerWithNullInstances() {
        JQuickGrpcRandomLoadBalancer balancer = new JQuickGrpcRandomLoadBalancer();
        assertNull(balancer.select(null));
        assertNull(balancer.select(new ArrayList<>()));
    }

    @Test
    void testRandomLoadBalancerWithSingleInstance() {
        JQuickGrpcRandomLoadBalancer balancer = new JQuickGrpcRandomLoadBalancer();
        List<JQuickGrpcServiceInstance> single = Arrays.asList(instances.get(0));
        JQuickGrpcServiceInstance selected = balancer.select(single);
        assertEquals(instances.get(0), selected);
    }

    @Test
    void testRoundRobinLoadBalancer() {
        JQuickGrpcRoundRobinLoadBalancer balancer = new JQuickGrpcRoundRobinLoadBalancer();
        // 多次选择应该轮询
        JQuickGrpcServiceInstance first = balancer.select(instances);
        JQuickGrpcServiceInstance second = balancer.select(instances);
        JQuickGrpcServiceInstance third = balancer.select(instances);
        JQuickGrpcServiceInstance fourth = balancer.select(instances);
        assertNotNull(first);
        assertNotNull(second);
        assertNotNull(third);
        assertNotNull(fourth);
        assertEquals(first, fourth);
    }

    @Test
    void testWeightedLoadBalancerRandom() {
        JQuickGrpcWeightedLoadBalancer balancer = new JQuickGrpcWeightedLoadBalancer(
                JQuickGrpcWeightedLoadBalancer.Algorithm.RANDOM
        );
        instances.get(0).setWeight(10);
        instances.get(1).setWeight(1);
        instances.get(2).setWeight(1);
        Map<String, Integer> selectionCount = new ConcurrentHashMap<>();
        for (int i = 0; i < 1000; i++) {
            JQuickGrpcServiceInstance selected = balancer.select(instances);
            selectionCount.merge(selected.getAddress(), 1, Integer::sum);
        }
        int count8080 = selectionCount.getOrDefault("localhost:8080", 0);
        int count8081 = selectionCount.getOrDefault("localhost:8081", 0);
        assertTrue(count8080 > count8081);
    }

    @Test
    void testWeightedLoadBalancerSmoothRR() {
        JQuickGrpcWeightedLoadBalancer balancer = new JQuickGrpcWeightedLoadBalancer(
                JQuickGrpcWeightedLoadBalancer.Algorithm.SMOOTH_RR
        );
        instances.get(0).setWeight(5);
        instances.get(1).setWeight(1);
        instances.get(2).setWeight(1);
        List<String> selections = new ArrayList<>();
        for (int i = 0; i < 14; i++) {
            JQuickGrpcServiceInstance selected = balancer.select(instances);
            selections.add(selected.getAddress());
        }
        long count8080 = selections.stream().filter(s -> s.equals("localhost:8080")).count();
        assertEquals(10, count8080);
    }

    @Test
    void testWeightedLoadBalancerSameWeight() {
        JQuickGrpcWeightedLoadBalancer balancer = new JQuickGrpcWeightedLoadBalancer();
        instances.forEach(i -> i.setWeight(1));
        JQuickGrpcServiceInstance selected = balancer.select(instances);
        assertNotNull(selected);
    }

    @Test
    void testWeightedLoadBalancerWithUnhealthyInstances() {
        JQuickGrpcWeightedLoadBalancer balancer = new JQuickGrpcWeightedLoadBalancer();
        instances.get(1).setHealthy(false);
        JQuickGrpcServiceInstance selected = balancer.select(instances);
        assertNotNull(selected);
        assertTrue(selected.isHealthy());
    }

    @Test
    void testLeastConnectionLoadBalancer() {
        JQuickGrpcLeastConnectionLoadBalancer balancer = new JQuickGrpcLeastConnectionLoadBalancer();
        // 增加某些实例的连接数
        balancer.incrementConnection(instances.get(0));
        balancer.incrementConnection(instances.get(0));
        balancer.incrementConnection(instances.get(1));
        JQuickGrpcServiceInstance selected = balancer.select(instances);
        // 连接数最少的应该是 instances.get(2)
        assertEquals(instances.get(2), selected);
        assertEquals("LeastConnection", balancer.getName());
    }

    @Test
    void testLeastConnectionLoadBalancerDecrement() {
        JQuickGrpcLeastConnectionLoadBalancer balancer = new JQuickGrpcLeastConnectionLoadBalancer();
        balancer.incrementConnection(instances.get(0));
        balancer.decrementConnection(instances.get(0));
        JQuickGrpcServiceInstance selected = balancer.select(instances);
        assertNotNull(selected);
    }
}
