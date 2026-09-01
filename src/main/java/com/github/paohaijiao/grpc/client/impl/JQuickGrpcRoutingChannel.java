package com.github.paohaijiao.grpc.client.impl;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.MethodDescriptor;

/**
 * JQuick routing channel (package-private).
 * Generated gRPC stubs are abstract classes, not interfaces, so JDK dynamic proxies cannot
 * be created for them. This channel lets a stub resolve the target instance on every RPC
 * ({@link #newCall}) through {@link TargetChannelProvider}, keeping per-call load balancing
 * while preserving the native stub API.
 *
 * <pre>{@code
 * Channel routing = new JQuickGrpcRoutingChannel("GreeterService",
 *         () -> selectInterceptedChannel("GreeterService"));
 * GreeterGrpc.GreeterBlockingStub stub = GreeterGrpc.newBlockingStub(routing);
 * }</pre>
 */
class JQuickGrpcRoutingChannel extends Channel {

    /**
     * Target channel provider: invoked on every newCall; performs instance selection
     * and channel acquisition internally. Should throw when no instance is available.
     */
    interface TargetChannelProvider {

        /**
         * Returns the currently selected target channel (with client interceptors attached).
         */
        Channel getTargetChannel();
    }

    private final String serviceName;

    private final TargetChannelProvider provider;

    JQuickGrpcRoutingChannel(String serviceName, TargetChannelProvider provider) {
        this.serviceName = serviceName;
        this.provider = provider;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(MethodDescriptor<ReqT, RespT> method, CallOptions callOptions) {
        return provider.getTargetChannel().newCall(method, callOptions);
    }

    @Override
    public String authority() {
        return "jquick-grpc://" + serviceName;
    }
}
