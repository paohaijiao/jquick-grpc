package com.github.paohaijiao.grpc.interceptor;

import com.github.paohaijiao.grpc.context.JQuickGrpcContext;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

public class JQuickGrpcServerInterceptor implements ServerInterceptor {

    private static final Metadata.Key<String> TRACE_ID_KEY = Metadata.Key.of("x-trace-id", Metadata.ASCII_STRING_MARSHALLER);

    private static final Metadata.Key<String> USER_ID_KEY = Metadata.Key.of("x-user-id", Metadata.ASCII_STRING_MARSHALLER);

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        String traceId = headers.get(TRACE_ID_KEY);
        String userId = headers.get(USER_ID_KEY);
        JQuickGrpcContext context = JQuickGrpcContext.create()
                .withTraceId(traceId)
                .withUserId(userId);
        try (JQuickGrpcContext.Scope ignored = context.attach()) {
            return next.startCall(call, headers);
        }
    }
}
