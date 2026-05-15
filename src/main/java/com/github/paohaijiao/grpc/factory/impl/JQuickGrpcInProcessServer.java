package com.github.paohaijiao.grpc.factory.impl;

import com.github.paohaijiao.grpc.config.JQuickGrpcServerConfig;
import com.github.paohaijiao.grpc.server.JQuickGrpcServer;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionService;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 进程内服务器，用于单元测试
 * 无网络开销，不需要实际端口
 */
public class JQuickGrpcInProcessServer implements JQuickGrpcServer {

    private final String serverName;

    private final JQuickGrpcServerConfig config;

    private final Map<String, BindableService> services;

    private final HealthStatusManager healthManager;

    private Server server;

    private volatile boolean running;

    public JQuickGrpcInProcessServer(JQuickGrpcServerConfig config) {
        this.config = config;
        this.serverName = "inprocess-server-" + System.currentTimeMillis();
        this.services = new ConcurrentHashMap<>();
        this.healthManager = new HealthStatusManager();
    }

    public JQuickGrpcInProcessServer(JQuickGrpcServerConfig config, String serverName) {
        this.config = config;
        this.serverName = serverName;
        this.services = new ConcurrentHashMap<>();
        this.healthManager = new HealthStatusManager();
    }

    @Override
    public void start() throws Exception {
        if (running) {
            return;
        }
        InProcessServerBuilder builder = InProcessServerBuilder.forName(serverName).directExecutor();  // 使用直接执行器，简化测试
        for (Map.Entry<String, BindableService> entry : services.entrySet()) {// 注册业务服务
            builder.addService(entry.getValue());
        }
        builder.addService(healthManager.getHealthService());// 注册健康检查和反射服务
        builder.addService(ProtoReflectionService.newInstance());
        this.server = builder.build();
        this.running = true;
        for (String serviceName : services.keySet()) {// 设置健康状态
            healthManager.setStatus(serviceName, ServingStatus.SERVING);
        }
        healthManager.setStatus("", ServingStatus.SERVING);
        server.start();
    }

    @Override
    public void stop() {
        if (!running || server == null) {
            return;
        }
        running = false;
        server.shutdown();
        try {
            server.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            server.shutdownNow();
        }
    }

    @Override
    public void registerService(BindableService service) {
        String serviceName = service.bindService().getServiceDescriptor().getName();
        registerService(serviceName, service);
    }

    @Override
    public void registerService(String serviceName, BindableService service) {
        services.put(serviceName, service);
        if (running && healthManager != null) {
            healthManager.setStatus(serviceName, ServingStatus.SERVING);
        }
    }

    @Override
    public void unregisterService(String serviceName) {
        services.remove(serviceName);
        if (running && healthManager != null) {
            healthManager.clearStatus(serviceName);
        }
    }

    @Override
    public Server getServer() {
        return server;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public Map<String, BindableService> getRegisteredServices() {
        return new ConcurrentHashMap<>(services);
    }

    @Override
    public int getPort() {
        return 0;  // InProcess 服务器没有端口
    }

    public String getServerName() {
        return serverName;
    }
}