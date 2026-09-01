package com.github.paohaijiao.grpc.server.impl;

import com.github.paohaijiao.grpc.config.JQuickGrpcServerConfig;
import com.github.paohaijiao.grpc.health.JQuickGrpcHealthStatusManager;
import com.github.paohaijiao.grpc.server.JQuickGrpcServer;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

public class JQuickGrpcNettyServer implements JQuickGrpcServer {

    private final JQuickGrpcServerConfig config;

    private final Map<String, BindableService> services;

    private final JQuickGrpcHealthStatusManager healthManager;

    private final List<ServerInterceptor> interceptors = new CopyOnWriteArrayList<>();

    private Server server;

    private volatile boolean running;

    private Thread shutdownHook;

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
        //  fail fast with a clear message when TLS is enabled without certificates
        if (!config.isUsePlaintext() && (config.getCertChainFile() == null || config.getPrivateKeyFile() == null)) {
            throw new IllegalStateException("TLS is enabled (usePlaintext=false) but certChainFile/privateKeyFile is not configured");
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
            builder.addService(entry.getValue());
        }
        for (ServerInterceptor interceptor : interceptors) {
            builder.intercept(interceptor);
        }
        builder.addService(healthManager.getHealthService());
        builder.addService(ProtoReflectionService.newInstance());
        this.server = builder.build();
        try {
            server.start();
        } catch (Exception e) {
            //  clear the reference on start failure to keep isRunning/stop consistent
            this.server = null;
            throw e;
        }
        this.running = true;
        registerShutdownHook();
    }

    /**
     * Registers the JVM shutdown hook (safe across repeated start/stop cycles).
     */
    private void registerShutdownHook() {
        try {
            shutdownHook = new Thread(() -> {
                System.out.println("Shutting down gRPC server...");
                JQuickGrpcNettyServer.this.stop();
            });
            Runtime.getRuntime().addShutdownHook(shutdownHook);
        } catch (IllegalStateException ignored) {
            // JVM is shutting down, hook registration not allowed
            shutdownHook = null;
        }
    }

    /**
     * Removes the registered shutdown hook to avoid hook accumulation leaks.
     */
    private void removeShutdownHook() {
        Thread hook = this.shutdownHook;
        this.shutdownHook = null;
        if (hook != null) {
            try {
                Runtime.getRuntime().removeShutdownHook(hook);
            } catch (IllegalStateException ignored) {
                //JVM is shutting down, ignore
            }
        }
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
        } finally {
            removeShutdownHook();
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
        //  when the configured port is 0 (ephemeral), return the actually bound port
        if (running && server != null) {
            int boundPort = server.getPort();
            if (boundPort > 0) {
                return boundPort;
            }
        }
        return config.getPort();
    }

    public JQuickGrpcHealthStatusManager getHealthManager() {
        return healthManager;
    }

    /**
     * Registers a server interceptor (must be called before start).
     */
    public void addInterceptor(ServerInterceptor interceptor) {
        interceptors.add(interceptor);
    }
}
