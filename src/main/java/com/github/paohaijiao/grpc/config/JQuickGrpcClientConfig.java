package com.github.paohaijiao.grpc.config;

import lombok.Data;

@Data
public class JQuickGrpcClientConfig {

    private String clientType = "pooled";  // pooled, single

    private int maxRetries = 3;

    private long retryDelayMillis = 1000;

    private int maxInboundMessageSize = 4 * 1024 * 1024;

    private long keepAliveTimeSeconds = 300;

    private boolean keepAliveWithoutCalls = false;

    private long deadlineMillis = 5000;

    private int maxConnections = Integer.MAX_VALUE;

    private int maxIdle = 5000;

    private int minIdle = 5000;

    private String compressionType = "gzip";  // gzip, snappy

    private boolean usePlaintext = true;

    private String target = "direct://localhost:9090";

    public static JQuickGrpcClientConfig pooled() {
        JQuickGrpcClientConfig config = new JQuickGrpcClientConfig();
        config.setClientType("pooled");
        return config;
    }


}