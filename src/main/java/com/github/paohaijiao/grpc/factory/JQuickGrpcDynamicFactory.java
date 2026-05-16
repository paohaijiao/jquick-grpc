package com.github.paohaijiao.grpc.factory;

import com.github.paohaijiao.grpc.client.JQuickGrpcClient;
import com.github.paohaijiao.grpc.client.impl.JQuickGrpcPooledClient;
import com.github.paohaijiao.grpc.client.impl.JQuickGrpcSingleClient;
import com.github.paohaijiao.grpc.config.JQuickGrpcClientConfig;
import com.github.paohaijiao.grpc.config.JQuickGrpcServerConfig;
import com.github.paohaijiao.grpc.discovery.JQuickGrpcServiceDiscovery;
import com.github.paohaijiao.grpc.loadbalance.JQuickGrpcLoadBalancer;
import com.github.paohaijiao.grpc.loadbalance.impl.JQuickGrpcLeastConnectionLoadBalancer;
import com.github.paohaijiao.grpc.loadbalance.impl.JQuickGrpcRandomLoadBalancer;
import com.github.paohaijiao.grpc.loadbalance.impl.JQuickGrpcRoundRobinLoadBalancer;
import com.github.paohaijiao.grpc.loadbalance.impl.JQuickGrpcWeightedLoadBalancer;
import com.github.paohaijiao.grpc.server.JQuickGrpcServer;
import com.github.paohaijiao.grpc.server.impl.JQuickGrpcNettyServer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class JQuickGrpcDynamicFactory {

    private final Map<String, JQuickGrpcServiceDiscovery> discoveryMap;

    private final Map<String, JQuickGrpcLoadBalancer> loadBalancerMap;

    private volatile JQuickGrpcServer activeServer;

    private volatile JQuickGrpcClient activeClient;

    public JQuickGrpcDynamicFactory() {
        this.discoveryMap = new ConcurrentHashMap<>();
        this.loadBalancerMap = new ConcurrentHashMap<>();
        initDefaultComponents();
    }

    private void initDefaultComponents() {
        loadBalancerMap.put("roundRobin", new JQuickGrpcRoundRobinLoadBalancer());
        loadBalancerMap.put("random", new JQuickGrpcRandomLoadBalancer());
        loadBalancerMap.put("weighted", new JQuickGrpcWeightedLoadBalancer());
        loadBalancerMap.put("leastConnection", new JQuickGrpcLeastConnectionLoadBalancer());
    }

    public JQuickGrpcServer createServer(JQuickGrpcServerConfig config) {
        this.activeServer = new JQuickGrpcNettyServer(config);
        return activeServer;
    }

    public JQuickGrpcClient createClient(JQuickGrpcClientConfig config, JQuickGrpcServiceDiscovery discovery, JQuickGrpcLoadBalancer loadBalancer) {
        switch (config.getClientType()) {
            case "pooled":
                this.activeClient = new JQuickGrpcPooledClient(config, discovery, loadBalancer);
                break;
            case "single":
                this.activeClient = new JQuickGrpcSingleClient(config, discovery, loadBalancer);
                break;
            default:
                this.activeClient = new JQuickGrpcPooledClient(config, discovery, loadBalancer);
        }
        return activeClient;
    }

    public JQuickGrpcLoadBalancer switchLoadBalancer(String type) {
        JQuickGrpcLoadBalancer balancer = loadBalancerMap.get(type);
        if (balancer == null) {
            throw new IllegalArgumentException("Unknown load balancer: " + type);
        }
        return balancer;
    }

    public void registerLoadBalancer(String name, JQuickGrpcLoadBalancer balancer) {
        loadBalancerMap.put(name, balancer);
    }

    public void registerDiscovery(String name, JQuickGrpcServiceDiscovery discovery) {
        discoveryMap.put(name, discovery);
    }

    public JQuickGrpcServer getActiveServer() {
        return activeServer;
    }

    public JQuickGrpcClient getActiveClient() {
        return activeClient;
    }

    public Map<String, Object> getCurrentStats() {
        Map<String, Object> stats = new java.util.HashMap<>();
        stats.put("activeServerRunning", activeServer != null && activeServer.isRunning());
        stats.put("activeClientClosed", activeClient != null && activeClient.isClosed());
        if (activeClient != null) {
            stats.put("clientStats", activeClient.getStats());
        }
        return stats;
    }
}
