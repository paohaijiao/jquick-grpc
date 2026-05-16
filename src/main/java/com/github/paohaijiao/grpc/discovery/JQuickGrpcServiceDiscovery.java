package com.github.paohaijiao.grpc.discovery;

import com.github.paohaijiao.grpc.domain.JQuickGrpcServiceInstance;

import java.util.List;

public interface JQuickGrpcServiceDiscovery {

    List<JQuickGrpcServiceInstance> getInstances(String serviceName);

    void subscribe(String serviceName, ServiceChangeListener listener);

    void unsubscribe(String serviceName, ServiceChangeListener listener);

    void close();

    interface ServiceChangeListener {
        void onChange(String serviceName, List<JQuickGrpcServiceInstance> instances);
    }
}
