package com.github.paohaijiao.grpc.loadbalance;

import com.github.paohaijiao.grpc.domain.JQuickGrpcServiceInstance;

import java.util.List;

public interface JQuickGrpcLoadBalancer {

    JQuickGrpcServiceInstance select(List<JQuickGrpcServiceInstance> instances);

    String getName();

}
