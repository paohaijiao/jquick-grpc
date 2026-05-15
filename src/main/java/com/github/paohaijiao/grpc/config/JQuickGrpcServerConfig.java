package com.github.paohaijiao.grpc.config;

import lombok.Data;

import java.io.File;

@Data
public class JQuickGrpcServerConfig {

    private int port = 9090;

    private int bossThreads = 1;

    private int workerThreads = 0;  // 默认 CPU 核心数 * 2

    private int maxInboundMessageSize = 4 * 1024 * 1024;  // 4MB

    private int keepAliveTimeMinutes = 5;

    private int keepAliveTimeoutSeconds = 20;

    private boolean permitKeepAliveWithoutCalls = false;

    private long handshakeTimeoutMillis = 5000;

    private boolean usePlaintext = true;  // 开发环境

    private File certChainFile;

    private File privateKeyFile;

    private boolean enableCompression = true;

    public static JQuickGrpcServerConfig defaultConfig() {
        return new JQuickGrpcServerConfig();
    }

    public static JQuickGrpcServerConfig secure(int port) {
        JQuickGrpcServerConfig config = new JQuickGrpcServerConfig();
        config.setPort(port);
        config.setUsePlaintext(false);
        return config;
    }

    public void setCertChainFilePath(String certChainFilePath) {
        if (certChainFilePath != null && !certChainFilePath.isEmpty()) {
            this.certChainFile = new File(certChainFilePath);
        }
    }

    public void setPrivateKeyFilePath(String privateKeyFilePath) {
        if (privateKeyFilePath != null && !privateKeyFilePath.isEmpty()) {
            this.privateKeyFile = new File(privateKeyFilePath);
        }
    }

    public void setCertChainResource(String resourcePath) {
        java.net.URL resource = getClass().getClassLoader().getResource(resourcePath);
        if (resource != null) {
            this.certChainFile = new File(resource.getFile());
        }
    }

    public void setCertChain(byte[] certChainBytes, byte[] privateKeyBytes) {
        this.certChainFile = createTempFile(certChainBytes, "cert");
        this.privateKeyFile = createTempFile(privateKeyBytes, "key");
    }

    private File createTempFile(byte[] data, String prefix) {
        try {
            File tempFile = File.createTempFile(prefix, ".tmp");
            tempFile.deleteOnExit();
            java.nio.file.Files.write(tempFile.toPath(), data);
            return tempFile;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create temp file for " + prefix, e);
        }
    }
}
