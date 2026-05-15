package com.github.paohaijiao;

import com.github.paohaijiao.grpc.config.JQuickGrpcClientConfig;
import com.github.paohaijiao.grpc.pool.JQuickGrpcChannelPool;
import io.grpc.ManagedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class JQuickGrpcChannelPoolTest {

    private JQuickGrpcChannelPool pool;

    private JQuickGrpcClientConfig config;

    @BeforeEach
    void setUp() {
        config = new JQuickGrpcClientConfig();
        config.setMaxConnections(5);
        config.setMaxIdle(3);
        config.setMinIdle(1);
        config.setUsePlaintext(true);
        pool = new JQuickGrpcChannelPool("localhost:9090", config);
    }

    @AfterEach
    void tearDown() {
        if (pool != null) {
            pool.close();
        }
    }

    @Test
    void testBorrowAndReturnObject() throws Exception {
        ManagedChannel channel = pool.borrowObject();
        assertNotNull(channel);
        assertEquals(1, pool.getActiveCount());
        pool.returnObject(channel);
    }

    @Test
    void testBorrowMultipleObjects() throws Exception {
        ManagedChannel channel1 = pool.borrowObject();
        ManagedChannel channel2 = pool.borrowObject();
        assertNotNull(channel1);
        assertNotNull(channel2);
        assertNotEquals(channel1, channel2);
        assertEquals(2, pool.getActiveCount());
        pool.returnObject(channel1);
        pool.returnObject(channel2);
    }

    @Test
    void testClose() throws Exception {
        ManagedChannel channel = pool.borrowObject();
        pool.returnObject(channel);
        pool.close();
        // 关闭后不能再借出
        assertThrows(Exception.class, () -> pool.borrowObject());
    }
}