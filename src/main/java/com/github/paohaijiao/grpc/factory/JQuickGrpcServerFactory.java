package com.github.paohaijiao.grpc.factory;

import com.github.paohaijiao.grpc.config.JQuickGrpcServerConfig;
import com.github.paohaijiao.grpc.server.JQuickGrpcServer;

public interface JQuickGrpcServerFactory {

    JQuickGrpcServer create(JQuickGrpcServerConfig config);

    String getServerType();
}
