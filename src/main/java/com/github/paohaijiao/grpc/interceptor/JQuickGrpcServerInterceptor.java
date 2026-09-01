package com.github.paohaijiao.grpc.interceptor;

import com.github.paohaijiao.grpc.context.JQuickGrpcContext;
import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

/**
 * Server interceptor: extracts x-trace-id / x-user-id from request headers and attaches
 * them to the gRPC context. Context attachment is re-applied for every listener callback
 * (onMessage/onHalfClose/...) because business logic runs after interceptCall returns.
 */
public class JQuickGrpcServerInterceptor implements ServerInterceptor {

    private static final Metadata.Key<String> TRACE_ID_KEY = Metadata.Key.of("x-trace-id", Metadata.ASCII_STRING_MARSHALLER);

    private static final Metadata.Key<String> USER_ID_KEY = Metadata.Key.of("x-user-id", Metadata.ASCII_STRING_MARSHALLER);

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        String traceId = headers.get(TRACE_ID_KEY);
        String userId = headers.get(USER_ID_KEY);
        JQuickGrpcContext context = JQuickGrpcContext.create().withTraceId(traceId).withUserId(userId);
        try (JQuickGrpcContext.Scope ignored = context.attach()) {
            ServerCall.Listener<ReqT> delegate = next.startCall(call, headers);
            //  keep the context visible during callbacks
            return new SimpleForwardingServerCallListener<ReqT>(delegate) {
                @Override
                public void onMessage(ReqT message) {
                    try (JQuickGrpcContext.Scope callbackScope = context.attach()) {
                        super.onMessage(message);
                    }
                }

                @Override
                public void onHalfClose() {
                    try (JQuickGrpcContext.Scope callbackScope = context.attach()) {
                        super.onHalfClose();
                    }
                }

                @Override
                public void onCancel() {
                    try (JQuickGrpcContext.Scope callbackScope = context.attach()) {
                        super.onCancel();
                    }
                }

                @Override
                public void onComplete() {
                    try (JQuickGrpcContext.Scope callbackScope = context.attach()) {
                        super.onComplete();
                    }
                }

                @Override
                public void onReady() {
                    try (JQuickGrpcContext.Scope callbackScope = context.attach()) {
                        super.onReady();
                    }
                }
            };
        }
    }
}
