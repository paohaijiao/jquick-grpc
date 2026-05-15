package com.github.paohaijiao.grpc.pool;

import com.github.paohaijiao.grpc.config.JQuickGrpcClientConfig;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.util.concurrent.TimeUnit;

public class JQuickGrpcChannelPool {

    private final GenericObjectPool<ManagedChannel> pool;

    private final String address;

    public JQuickGrpcChannelPool(String address, JQuickGrpcClientConfig config) {
        this.address = address;
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxTotal(config.getMaxConnections());
        poolConfig.setMaxIdle(config.getMaxIdle());
        poolConfig.setMinIdle(config.getMinIdle());
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        this.pool = new GenericObjectPool<>(new BasePooledObjectFactory<ManagedChannel>() {
            @Override
            public ManagedChannel create() throws Exception {
                return createChannel(address, config);
            }

            @Override
            public PooledObject<ManagedChannel> wrap(ManagedChannel channel) {
                return new DefaultPooledObject<>(channel);
            }

            @Override
            public void destroyObject(PooledObject<ManagedChannel> p) {
                p.getObject().shutdown();
                try {
                    p.getObject().awaitTermination(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            @Override
            public boolean validateObject(PooledObject<ManagedChannel> p) {
                return !p.getObject().isShutdown() && !p.getObject().isTerminated();
            }
        }, poolConfig);
    }

    private ManagedChannel createChannel(String address, JQuickGrpcClientConfig config) {
        String[] parts = address.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);
        NettyChannelBuilder builder = NettyChannelBuilder.forAddress(host, port);
        if (config.isUsePlaintext()) {
            builder.usePlaintext();
        }
        builder.maxInboundMessageSize(config.getMaxInboundMessageSize())
                .keepAliveTime(config.getKeepAliveTimeSeconds(), TimeUnit.SECONDS)
                .keepAliveWithoutCalls(config.isKeepAliveWithoutCalls());
        return builder.build();
    }

    public ManagedChannel borrowObject() throws Exception {
        return pool.borrowObject();
    }

    public void returnObject(ManagedChannel channel) {
        if (channel != null) {
            pool.returnObject(channel);
        }
    }

    public void close() {
        pool.close();
    }

    public int getActiveCount() {
        return pool.getNumActive();
    }
}
