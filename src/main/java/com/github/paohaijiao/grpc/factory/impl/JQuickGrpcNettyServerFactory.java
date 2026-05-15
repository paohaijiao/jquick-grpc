package com.github.paohaijiao.grpc.factory.impl;

import com.github.paohaijiao.grpc.config.JQuickGrpcServerConfig;
import com.github.paohaijiao.grpc.factory.JQuickGrpcServerFactory;
import com.github.paohaijiao.grpc.server.JQuickGrpcServer;
import com.github.paohaijiao.grpc.server.impl.JQuickGrpcNettyServer;

public class JQuickGrpcNettyServerFactory implements JQuickGrpcServerFactory {

    @Override
    public JQuickGrpcServer create(JQuickGrpcServerConfig config) {
        return new JQuickGrpcNettyServer(config);
    }

    @Override
    public String getServerType() {
        return "netty";
    }
}