package com.github.paohaijiao;


import com.github.paohaijiao.grpc.context.JQuickGrpcContext;
import io.grpc.Context;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class JQuickGrpcContextTest {

    @Test
    void testCreate() {
        JQuickGrpcContext context = JQuickGrpcContext.create();
        assertNotNull(context);
    }

    @Test
    void testWithTraceId() {
        JQuickGrpcContext context = JQuickGrpcContext.create().withTraceId("trace-123");
        try (JQuickGrpcContext.Scope scope = context.attach()) {
            assertEquals("trace-123", JQuickGrpcContext.getTraceId());
        }
    }

    @Test
    void testWithUserId() {
        JQuickGrpcContext context = JQuickGrpcContext.create().withUserId("user-456");
        try (JQuickGrpcContext.Scope scope = context.attach()) {
            assertEquals("user-456", JQuickGrpcContext.getUserId());
        }
    }

    @Test
    void testWithAuthToken() {
        JQuickGrpcContext context = JQuickGrpcContext.create().withAuthToken("token-789");
        try (JQuickGrpcContext.Scope scope = context.attach()) {
            assertEquals("token-789", JQuickGrpcContext.getAuthToken());
        }
    }

    @Test
    void testChainedContext() {
        JQuickGrpcContext context = JQuickGrpcContext.create()
                .withTraceId("trace-123")
                .withUserId("user-456")
                .withAuthToken("token-789");

        try (JQuickGrpcContext.Scope scope = context.attach()) {
            assertEquals("trace-123", JQuickGrpcContext.getTraceId());
            assertEquals("user-456", JQuickGrpcContext.getUserId());
            assertEquals("token-789", JQuickGrpcContext.getAuthToken());
        }
    }

    @Test
    void testScopeAutoClose() {
        JQuickGrpcContext context = JQuickGrpcContext.create()
                .withTraceId("trace-123");
        String originalTraceId = JQuickGrpcContext.getTraceId();
        try (JQuickGrpcContext.Scope scope = context.attach()) {
            assertEquals("trace-123", JQuickGrpcContext.getTraceId());
        }

        assertEquals(originalTraceId, JQuickGrpcContext.getTraceId());
    }
}
