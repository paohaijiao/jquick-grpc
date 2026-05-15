package com.github.paohaijiao.grpc.server;

import io.grpc.BindableService;
import io.grpc.Server;

import java.util.Map;

public interface JQuickGrpcServer {

    void start() throws Exception;

    void stop();

    void registerService(BindableService service);

    void registerService(String serviceName, BindableService service);

    void unregisterService(String serviceName);

    Server getServer();

    boolean isRunning();

    Map<String, BindableService> getRegisteredServices();

    int getPort();
}
