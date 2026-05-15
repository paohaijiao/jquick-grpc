package com.github.paohaijiao;

import com.github.paohaijiao.grpc.config.JQuickGrpcServerConfig;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

class JQuickGrpcServerConfigTest {

    @Test
    void testDefaultConfig() {
        JQuickGrpcServerConfig config = JQuickGrpcServerConfig.defaultConfig();

        assertEquals(9090, config.getPort());
        assertEquals(1, config.getBossThreads());
        assertEquals(0, config.getWorkerThreads());
        assertEquals(4 * 1024 * 1024, config.getMaxInboundMessageSize());
        assertEquals(5, config.getKeepAliveTimeMinutes());
        assertEquals(20, config.getKeepAliveTimeoutSeconds());
        assertFalse(config.isPermitKeepAliveWithoutCalls());
        assertEquals(5000, config.getHandshakeTimeoutMillis());
        assertTrue(config.isUsePlaintext());
        assertTrue(config.isEnableCompression());
    }

    @Test
    void testSecureConfig() {
        JQuickGrpcServerConfig config = JQuickGrpcServerConfig.secure(8443);

        assertEquals(8443, config.getPort());
        assertFalse(config.isUsePlaintext());
    }

    @Test
    void testCertChainFilePath() {
        JQuickGrpcServerConfig config = new JQuickGrpcServerConfig();
        config.setCertChainFilePath("/path/to/cert.pem");

        assertNotNull(config.getCertChainFile());
        assertEquals("/path/to/cert.pem", config.getCertChainFile().getPath());
    }

    @Test
    void testPrivateKeyFilePath() {
        JQuickGrpcServerConfig config = new JQuickGrpcServerConfig();
        config.setPrivateKeyFilePath("/path/to/key.pem");

        assertNotNull(config.getPrivateKeyFile());
        assertEquals("/path/to/key.pem", config.getPrivateKeyFile().getPath());
    }

    @Test
    void testSetCertChain() {
        JQuickGrpcServerConfig config = new JQuickGrpcServerConfig();
        byte[] certBytes = "certificate".getBytes();
        byte[] keyBytes = "privatekey".getBytes();

        config.setCertChain(certBytes, keyBytes);

        assertNotNull(config.getCertChainFile());
        assertNotNull(config.getPrivateKeyFile());
        assertTrue(config.getCertChainFile().exists());
        assertTrue(config.getPrivateKeyFile().exists());
    }
}
