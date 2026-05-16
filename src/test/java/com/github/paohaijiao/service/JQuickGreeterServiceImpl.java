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
 *
 * Copyright (c) [2025-2099] Martin (goudingcheng@gmail.com)
 */
package com.github.paohaijiao.service;

/**
 * packageName com.github.paohaijiao.service
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/16
 */
import com.github.paohaijiao.grpc.annotation.JQuickGrpcService;
import com.github.paohaijiao.grpc.test.GreeterGrpc;
import com.github.paohaijiao.grpc.test.GreeterProto;
import io.grpc.stub.StreamObserver;

@JQuickGrpcService(name = "Greeter", version = 1)
public class JQuickGreeterServiceImpl extends GreeterGrpc.GreeterImplBase {

    @Override
    public void sayHello(GreeterProto.HelloRequest request, StreamObserver<GreeterProto.HelloReply> responseObserver) {
        String name = request.getName();
        String message = "Hello, " + name + "!";
        GreeterProto.HelloReply reply = GreeterProto.HelloReply.newBuilder().setMessage(message).build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }
}
