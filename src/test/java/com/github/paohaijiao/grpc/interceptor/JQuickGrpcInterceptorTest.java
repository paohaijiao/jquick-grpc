/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.paohaijiao.grpc.interceptor;

import com.github.paohaijiao.grpc.config.JQuickGrpcServerConfig;
import com.github.paohaijiao.grpc.context.JQuickGrpcContext;
import com.github.paohaijiao.grpc.factory.impl.JQuickGrpcInProcessServer;
import com.github.paohaijiao.grpc.test.GreeterGrpc;
import com.github.paohaijiao.grpc.test.GreeterProto;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * <p>
 * End-to-end interceptor tests: the client interceptor propagates traceId/userId
 * from JQuickGrpcContext into request headers; the server interceptor restores
 * them into the server-side context so business code can read them.
 */
@DisplayName("拦截器链路 / interceptor chain")
class JQuickGrpcInterceptorTest {

    private JQuickGrpcInProcessServer server;

    private ManagedChannel channel;

    @BeforeEach
    void setUp() throws Exception {
        server = new JQuickGrpcInProcessServer(new JQuickGrpcServerConfig());
        server.addInterceptor(new JQuickGrpcServerInterceptor());
        server.registerService(new ContextEchoGreeter());
        server.start();
        channel = InProcessChannelBuilder.forName(server.getServerName()).directExecutor()
                .intercept(new JQuickGrpcClientInterceptor())
                .build();
    }

    @AfterEach
    void tearDown() {
        channel.shutdownNow();
        server.stop();
    }

    @Test
    @DisplayName("traceId/userId 经客户端拦截器传播到服务端业务线程 / context propagation end to end")
    void contextIsPropagatedEndToEnd() {
        GreeterGrpc.GreeterBlockingStub stub = GreeterGrpc.newBlockingStub(channel);
        try (JQuickGrpcContext.Scope scope = JQuickGrpcContext.create().withTraceId("trace-123").withUserId("user-42").attach()) {
            GreeterProto.HelloReply reply = stub.sayHello(GreeterProto.HelloRequest.newBuilder().setName("x").build());
            assertThat(reply.getMessage()).isEqualTo("trace=trace-123,user=user-42");
        }
    }

    @Test
    @DisplayName("无上下文时服务端读到 null 且不抛异常 / missing context yields nulls without errors")
    void missingContextYieldsNulls() {
        GreeterGrpc.GreeterBlockingStub stub = GreeterGrpc.newBlockingStub(channel);
        GreeterProto.HelloReply reply = stub.sayHello(GreeterProto.HelloRequest.newBuilder().setName("x").build());
        assertThat(reply.getMessage()).isEqualTo("trace=null,user=null");
    }

    @Test
    @DisplayName("嵌套作用域关闭后上下文恢复 / nested scopes restore previous context")
    void nestedScopesRestoreContext() {
        GreeterGrpc.GreeterBlockingStub stub = GreeterGrpc.newBlockingStub(channel);
        try (JQuickGrpcContext.Scope outer = JQuickGrpcContext.create().withTraceId("outer").attach()) {
            assertThat(JQuickGrpcContext.getTraceId()).isEqualTo("outer");
            try (JQuickGrpcContext.Scope inner = JQuickGrpcContext.create().withTraceId("inner").attach()) {
                GreeterProto.HelloReply reply = stub.sayHello(GreeterProto.HelloRequest.newBuilder().setName("x").build());
                assertThat(reply.getMessage()).isEqualTo("trace=inner,user=null");
            }
            // 内层关闭后恢复为 outer / inner scope restored the outer context
            assertThat(JQuickGrpcContext.getTraceId()).isEqualTo("outer");
        }
        assertThat(JQuickGrpcContext.getTraceId()).isNull();
    }

    /**
     * Echoes the server-side traceId/userId back in the response message for assertions.
     */
    private static final class ContextEchoGreeter extends GreeterGrpc.GreeterImplBase {

        @Override
        public void sayHello(GreeterProto.HelloRequest request, StreamObserver<GreeterProto.HelloReply> responseObserver) {
            String message = "trace=" + JQuickGrpcContext.getTraceId() + ",user=" + JQuickGrpcContext.getUserId();
            responseObserver.onNext(GreeterProto.HelloReply.newBuilder().setMessage(message).build());
            responseObserver.onCompleted();
        }
    }

    /**
     * A header-capturing interceptor proving that the client interceptor writes metadata.
     */
    static final class HeaderCapturingServerInterceptor implements ServerInterceptor {

        static final Metadata.Key<String> TRACE_ID_KEY = Metadata.Key.of("x-trace-id", Metadata.ASCII_STRING_MARSHALLER);

        private volatile String capturedTraceId;

        @Override
        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
            capturedTraceId = headers.get(TRACE_ID_KEY);
            return next.startCall(call, headers);
        }

        String getCapturedTraceId() {
            return capturedTraceId;
        }
    }

    @Test
    @DisplayName("客户端拦截器写入 x-trace-id 请求头 / client interceptor writes the x-trace-id header")
    void clientInterceptorWritesHeader() throws Exception {
        JQuickGrpcInProcessServer rawServer = new JQuickGrpcInProcessServer(new JQuickGrpcServerConfig());
        HeaderCapturingServerInterceptor capturer = new HeaderCapturingServerInterceptor();
        rawServer.addInterceptor(capturer);
        rawServer.registerService(new ContextEchoGreeter());
        rawServer.start();
        ManagedChannel rawChannel = InProcessChannelBuilder.forName(rawServer.getServerName()).directExecutor()
                .intercept(new JQuickGrpcClientInterceptor())
                .build();
        try {
            GreeterGrpc.GreeterBlockingStub stub = GreeterGrpc.newBlockingStub(rawChannel);
            try (JQuickGrpcContext.Scope scope = JQuickGrpcContext.create().withTraceId("header-check").attach()) {
                stub.sayHello(GreeterProto.HelloRequest.newBuilder().setName("x").build());
            }
            assertThat(capturer.getCapturedTraceId()).isEqualTo("header-check");
        } finally {
            rawChannel.shutdownNow();
            rawServer.stop();
        }
    }
}
