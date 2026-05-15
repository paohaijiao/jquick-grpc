package com.github.paohaijiao;


import com.github.paohaijiao.grpc.config.JQuickGrpcClientConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class JQuickGrpcClientConfigTest {

    @Test
    void testDefaultValues() {
        JQuickGrpcClientConfig config = new JQuickGrpcClientConfig();
        assertEquals("pooled", config.getClientType());
        assertEquals(3, config.getMaxRetries());
        assertEquals(1000, config.getRetryDelayMillis());
        assertEquals(4 * 1024 * 1024, config.getMaxInboundMessageSize());
        assertEquals(300, config.getKeepAliveTimeSeconds());
        assertFalse(config.isKeepAliveWithoutCalls());
        assertEquals(5000, config.getDeadlineMillis());
        assertTrue(config.isUsePlaintext());
    }

    @Test
    void testPooledFactoryMethod() {
        JQuickGrpcClientConfig config = JQuickGrpcClientConfig.pooled();
        assertEquals("pooled", config.getClientType());
    }

    @Test
    void testSetters() {
        JQuickGrpcClientConfig config = new JQuickGrpcClientConfig();

        config.setClientType("single");
        config.setMaxRetries(5);
        config.setRetryDelayMillis(500);
        config.setDeadlineMillis(10000);
        config.setUsePlaintext(false);

        assertEquals("single", config.getClientType());
        assertEquals(5, config.getMaxRetries());
        assertEquals(500, config.getRetryDelayMillis());
        assertEquals(10000, config.getDeadlineMillis());
        assertFalse(config.isUsePlaintext());
    }
}