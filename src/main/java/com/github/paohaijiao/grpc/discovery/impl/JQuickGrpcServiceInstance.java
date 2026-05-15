package com.github.paohaijiao.grpc.discovery.impl;
import com.github.paohaijiao.grpc.metadata.JQuickServiceInstanceMetrics;
import lombok.Data;

@Data
public class JQuickGrpcServiceInstance {

    private String serviceName;

    private String host;

    private int port;

    private int weight = 1;

    private boolean healthy = true;

    private JQuickServiceInstanceMetrics metrics;

    public JQuickGrpcServiceInstance(String serviceName, String host, int port) {
        this.serviceName = serviceName;
        this.host = host;
        this.port = port;
    }

    public String getAddress() {
        return host + ":" + port;
    }
}
