package com.github.paohaijiao.grpc.client.impl;

import io.grpc.Channel;

import java.lang.reflect.Method;

/**
 * gRPC Stub static factory helper (package-private).
 * Generated stubs (e.g. {@code GreeterGrpc.GreeterBlockingStub}) are inner classes of the
 * generated outer class (e.g. {@code GreeterGrpc}) with private constructors; they must be
 * created through the static factory methods declared on the outer class. Looking up
 * {@code newStub} directly on the inner class throws {@link NoSuchMethodException}.
 *
 * <pre>{@code
 * Object stub = JQuickGrpcStubFactory.newStub(GreeterGrpc.GreeterBlockingStub.class, channel);
 * GreeterProto.HelloReply reply =
 *         ((GreeterGrpc.GreeterBlockingStub) stub).sayHello(request);
 * }</pre>
 */
final class JQuickGrpcStubFactory {

    private JQuickGrpcStubFactory() {
    }

    /**
     * Creates a stub instance by resolving the correct static factory method for the given stub type.
     * <p>
     * 映射规则 / mapping rules:
     * <ul>
     *     <li>{@code XxxBlockingV2Stub} -&gt; {@code XxxGrpc.newBlockingV2Stub(Channel)}</li>
     *     <li>{@code XxxBlockingStub}   -&gt; {@code XxxGrpc.newBlockingStub(Channel)}</li>
     *     <li>{@code XxxFutureStub}     -&gt; {@code XxxGrpc.newFutureStub(Channel)}</li>
     *     <li>其他（异步 Stub）       -&gt; {@code XxxGrpc.newStub(Channel)}</li>
     * </ul>
     *
     * @param stubClass Stub  the stub class, e.g. GreeterGrpc.GreeterBlockingStub
     * @param channel   the target channel
     * @return  the stub bound to the given channel
     * @throws Exception  thrown when reflective invocation fails
     */
    static Object newStub(Class<?> stubClass, Channel channel) throws Exception {
        Class<?> outerClass = stubClass.getEnclosingClass();
        String simpleName = stubClass.getSimpleName();
        String factoryMethod;
        if (simpleName.endsWith("BlockingV2Stub")) {
            factoryMethod = "newBlockingV2Stub";
        } else if (simpleName.endsWith("BlockingStub")) {
            factoryMethod = "newBlockingStub";
        } else if (simpleName.endsWith("FutureStub")) {
            factoryMethod = "newFutureStub";
        } else {
            factoryMethod = "newStub";
        }
        if (outerClass != null) {
            Method factory = outerClass.getMethod(factoryMethod, Channel.class);
            return factory.invoke(null, channel);
        }
        Method legacy = stubClass.getMethod("newStub", Channel.class);
        return legacy.invoke(null, channel);
    }
}
