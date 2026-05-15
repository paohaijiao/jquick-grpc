package com.github.paohaijiao.grpc.client;

import java.util.Map;

public interface JQuickGrpcClient {

    <T> T getService(Class<T> serviceClass, String serviceName);

    <T> T getServiceWithDeadline(Class<T> serviceClass, String serviceName, long deadlineMillis);

    void close();

    Map<String, Object> getStats();

    String getClientType();

    boolean isClosed();
}
