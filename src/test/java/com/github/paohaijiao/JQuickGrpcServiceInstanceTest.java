package com.github.paohaijiao;
import com.github.paohaijiao.grpc.discovery.impl.JQuickGrpcServiceInstance;
import com.github.paohaijiao.grpc.metadata.JQuickServiceInstanceMetrics;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
public class JQuickGrpcServiceInstanceTest {

    @Test
    public void testConstructor() {
        JQuickGrpcServiceInstance instance = new JQuickGrpcServiceInstance("my-service", "192.168.1.1", 9090);
        assertEquals("my-service", instance.getServiceName());
        assertEquals("192.168.1.1", instance.getHost());
        assertEquals(9090, instance.getPort());
    }

    @Test
    void testGetAddress() {
        JQuickGrpcServiceInstance instance = new JQuickGrpcServiceInstance("test", "localhost", 8080);
        assertEquals("localhost:8080", instance.getAddress());
    }

    @Test
    void testSetters() {
        JQuickGrpcServiceInstance instance = new JQuickGrpcServiceInstance("test", "localhost", 8080);
        instance.setWeight(10);
        instance.setHealthy(false);
        assertEquals(10, instance.getWeight());
        assertFalse(instance.isHealthy());
    }

    @Test
    void testMetrics() {
        JQuickGrpcServiceInstance instance = new JQuickGrpcServiceInstance("test", "localhost", 8080);
        JQuickServiceInstanceMetrics metrics = new JQuickServiceInstanceMetrics();
        metrics.setCpuUsage(0.5);
        metrics.setMemoryUsage(0.6);
        instance.setMetrics(metrics);
        assertNotNull(instance.getMetrics());
        assertEquals(0.5, instance.getMetrics().getCpuUsage());
        assertEquals(0.6, instance.getMetrics().getMemoryUsage());
    }
}