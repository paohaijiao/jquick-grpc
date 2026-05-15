package com.github.paohaijiao;
// JQuickGrpcHealthStatusManagerTest.java

import com.github.paohaijiao.grpc.health.JQuickGrpcHealthStatusManager;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class JQuickGrpcHealthStatusManagerTest {

    @Test
    void testSetStatus() {
        JQuickGrpcHealthStatusManager manager = new JQuickGrpcHealthStatusManager();
        manager.setStatus("test-service", JQuickGrpcHealthStatusManager.Status.SERVING);
        manager.setStatus("another-service", JQuickGrpcHealthStatusManager.Status.NOT_SERVING);
        assertNotNull(manager.getHealthService());
    }

    @Test
    void testStatusEnum() {
        assertEquals(
                io.grpc.health.v1.HealthCheckResponse.ServingStatus.SERVING,
                JQuickGrpcHealthStatusManager.Status.SERVING.toGrpcStatus()
        );
        assertEquals(
                io.grpc.health.v1.HealthCheckResponse.ServingStatus.NOT_SERVING,
                JQuickGrpcHealthStatusManager.Status.NOT_SERVING.toGrpcStatus()
        );
        assertEquals(
                io.grpc.health.v1.HealthCheckResponse.ServingStatus.UNKNOWN,
                JQuickGrpcHealthStatusManager.Status.UNKNOWN.toGrpcStatus()
        );
        assertEquals(
                io.grpc.health.v1.HealthCheckResponse.ServingStatus.SERVICE_UNKNOWN,
                JQuickGrpcHealthStatusManager.Status.SERVICE_UNKNOWN.toGrpcStatus()
        );
    }
}