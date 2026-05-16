package com.github.paohaijiao.grpc.server.impl;

import com.github.paohaijiao.grpc.config.JQuickGrpcServerConfig;
import com.github.paohaijiao.grpc.health.JQuickGrpcHealthStatusManager;
import com.github.paohaijiao.grpc.server.JQuickGrpcServer;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class JQuickGrpcNettyServer implements JQuickGrpcServer {

    private final JQuickGrpcServerConfig config;

    private final Map<String, BindableService> services;

    private final JQuickGrpcHealthStatusManager healthManager;

    private Server server;

    private volatile boolean running;

    public JQuickGrpcNettyServer(JQuickGrpcServerConfig config) {
        this.config = config;
        this.services = new ConcurrentHashMap<>();
        this.healthManager = new JQuickGrpcHealthStatusManager();
    }

    @Override
    public void start() throws Exception {
        if (running) {
            return;
        }
        ServerBuilder<?> builder = NettyServerBuilder.forPort(config.getPort())
                .maxInboundMessageSize(config.getMaxInboundMessageSize())
                .keepAliveTime(config.getKeepAliveTimeMinutes(), TimeUnit.MINUTES)
                .keepAliveTimeout(config.getKeepAliveTimeoutSeconds(), TimeUnit.SECONDS)
                .permitKeepAliveWithoutCalls(config.isPermitKeepAliveWithoutCalls())
                .handshakeTimeout(config.getHandshakeTimeoutMillis(), TimeUnit.MILLISECONDS);

        if (!config.isUsePlaintext()) {
            NettyServerBuilder nettyBuilder = (NettyServerBuilder) builder;
            nettyBuilder.useTransportSecurity(
                    config.getCertChainFile(),
                    config.getPrivateKeyFile()
            );
        }
        for (Map.Entry<String, BindableService> entry : services.entrySet()) {
            builder.addService(entry.getValue());// 注册服务
        }
        builder.addService(healthManager.getHealthService());// 注册健康检查服务
        builder.addService(ProtoReflectionService.newInstance());// 注册反射服务（便于调试）
        this.server = builder.build();
        this.running = true;
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {// 添加 shutdown hook
            System.out.println("Shutting down gRPC server...");
            JQuickGrpcNettyServer.this.stop();
        }));
    }

    @Override
    public void stop() {
        if (!running || server == null) {
            return;
        }
        running = false;
        server.shutdown();
        try {
            server.awaitTermination(30, TimeUnit.SECONDS);
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
        healthManager.setStatus(serviceName, JQuickGrpcHealthStatusManager.Status.SERVING);
    }

    @Override
    public void unregisterService(String serviceName) {
        services.remove(serviceName);
        healthManager.setStatus(serviceName, JQuickGrpcHealthStatusManager.Status.NOT_SERVING);
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
        return config.getPort();
    }

    public JQuickGrpcHealthStatusManager getHealthManager() {
        return healthManager;
    }
}
