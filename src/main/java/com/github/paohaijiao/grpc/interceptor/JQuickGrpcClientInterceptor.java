package com.github.paohaijiao.grpc.interceptor;


import com.github.paohaijiao.grpc.context.JQuickGrpcContext;
import io.grpc.*;

public class JQuickGrpcClientInterceptor implements ClientInterceptor {

    private static final Metadata.Key<String> TRACE_ID_KEY = Metadata.Key.of("x-trace-id", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> USER_ID_KEY = Metadata.Key.of("x-user-id", Metadata.ASCII_STRING_MARSHALLER);

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                // 传递上下文元数据
                String traceId = JQuickGrpcContext.getTraceId();
                if (traceId != null) {
                    headers.put(TRACE_ID_KEY, traceId);
                }
                String userId = JQuickGrpcContext.getUserId();
                if (userId != null) {
                    headers.put(USER_ID_KEY, userId);
                }

                super.start(responseListener, headers);
            }
        };
    }
}
