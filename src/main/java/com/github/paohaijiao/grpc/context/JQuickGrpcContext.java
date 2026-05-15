package com.github.paohaijiao.grpc.context;

import io.grpc.Context;

public class JQuickGrpcContext {

    private static final Context.Key<String> TRACE_ID_KEY = Context.key("trace-id");

    private static final Context.Key<String> USER_ID_KEY = Context.key("user-id");

    private static final Context.Key<String> AUTH_TOKEN_KEY = Context.key("auth-token");

    private final Context context;

    private JQuickGrpcContext(Context context) {
        this.context = context;
    }

    public static JQuickGrpcContext create() {
        return new JQuickGrpcContext(Context.current());
    }

    public static String getTraceId() {
        return TRACE_ID_KEY.get();
    }

    public static String getUserId() {
        return USER_ID_KEY.get();
    }

    public static String getAuthToken() {
        return AUTH_TOKEN_KEY.get();
    }

    public JQuickGrpcContext withTraceId(String traceId) {
        return new JQuickGrpcContext(context.withValue(TRACE_ID_KEY, traceId));
    }

    public JQuickGrpcContext withUserId(String userId) {
        return new JQuickGrpcContext(context.withValue(USER_ID_KEY, userId));
    }

    public JQuickGrpcContext withAuthToken(String token) {
        return new JQuickGrpcContext(context.withValue(AUTH_TOKEN_KEY, token));
    }

    public Scope attach() {
        return new Scope(context.attach());
    }

    public class Scope implements AutoCloseable {
        private final Context previous;

        private Scope(Context previous) {
            this.previous = previous;
        }

        @Override
        public void close() {
            Context.current().detach(previous);
        }
    }
}
