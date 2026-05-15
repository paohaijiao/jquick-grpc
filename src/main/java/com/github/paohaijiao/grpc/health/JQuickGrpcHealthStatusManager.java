package com.github.paohaijiao.grpc.health;

import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.services.HealthStatusManager;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class JQuickGrpcHealthStatusManager {

    private final HealthStatusManager healthManager;

    private final Map<String, HealthCheckResponse.ServingStatus> statusMap;

    public JQuickGrpcHealthStatusManager() {
        this.healthManager = new HealthStatusManager();
        this.statusMap = new ConcurrentHashMap<>();
    }

    public void setStatus(String serviceName, Status status) {
        HealthCheckResponse.ServingStatus grpcStatus = status.toGrpcStatus();
        statusMap.put(serviceName, grpcStatus);
        healthManager.setStatus(serviceName, grpcStatus);
    }

    public io.grpc.BindableService getHealthService() {
        return healthManager.getHealthService();
    }

    public enum Status {
        SERVING, NOT_SERVING, UNKNOWN, SERVICE_UNKNOWN;
        public HealthCheckResponse.ServingStatus toGrpcStatus() {
            switch (this) {
                case SERVING:
                    return HealthCheckResponse.ServingStatus.SERVING;
                case NOT_SERVING:
                    return HealthCheckResponse.ServingStatus.NOT_SERVING;
                case UNKNOWN:
                    return HealthCheckResponse.ServingStatus.UNKNOWN;
                default:
                    return HealthCheckResponse.ServingStatus.SERVICE_UNKNOWN;
            }
        }
    }
}
