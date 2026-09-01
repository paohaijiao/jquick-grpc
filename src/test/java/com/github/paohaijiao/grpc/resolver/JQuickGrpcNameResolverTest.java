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
package com.github.paohaijiao.grpc.resolver;

import com.github.paohaijiao.grpc.discovery.JQuickGrpcServiceDiscovery;
import com.github.paohaijiao.grpc.domain.JQuickGrpcServiceInstance;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import io.grpc.Status;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * JQuickGrpcNameResolver unit tests with a mocked discovery and gRPC listener
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("JQuickGrpcNameResolver 名称解析 / name resolver")
class JQuickGrpcNameResolverTest {

    @Mock
    private JQuickGrpcServiceDiscovery discovery;

    @Mock
    private NameResolver.Listener2 listener;

    private JQuickGrpcNameResolver resolver;

    @BeforeEach
    void setUp() {
        Executor directExecutor = Runnable::run;
        resolver = new JQuickGrpcNameResolver("GreeterService", discovery, directExecutor);
    }

    private static JQuickGrpcServiceInstance instance(String host, int port) {
        return new JQuickGrpcServiceInstance("GreeterService", host, port);
    }

    @Test
    @DisplayName("服务权限为服务名 / service authority equals the service name")
    void serviceAuthorityIsServiceName() {
        assertThat(resolver.getServiceAuthority()).isEqualTo("GreeterService");
    }

    @Test
    @DisplayName("start 后解析出地址列表 / start resolves the discovered addresses")
    void startResolvesAddresses() {
        when(discovery.getInstances("GreeterService"))
                .thenReturn(Arrays.asList(instance("127.0.0.1", 9000), instance("127.0.0.1", 9001)));

        resolver.start(listener);

        ArgumentCaptor<NameResolver.ResolutionResult> captor = ArgumentCaptor.forClass(NameResolver.ResolutionResult.class);
        verify(listener).onResult(captor.capture());
        List<EquivalentAddressGroup> addresses = captor.getValue().getAddresses();
        assertThat(addresses).hasSize(2);
        verify(discovery).subscribe(eq("GreeterService"), any());
    }

    @Test
    @DisplayName("无实例时回调 NOT_FOUND / reports NOT_FOUND when no instances exist")
    void reportsNotFoundWithoutInstances() {
        when(discovery.getInstances("GreeterService")).thenReturn(Collections.emptyList());

        resolver.start(listener);

        //  only the status code matters here
        verify(listener).onError(org.mockito.ArgumentMatchers.argThat(
                status -> status != null && status.getCode() == Status.Code.NOT_FOUND));
    }

    @Test
    @DisplayName("start 前 refresh 安全（修复 NPE）/ refresh before start is safe")
    void refreshBeforeStartIsSafe() {
        assertThatCode(resolver::refresh).doesNotThrowAnyException();
        verifyNoInteractions(discovery);
    }

    @Test
    @DisplayName("shutdown 退订同一个监听器实例（修复订阅泄漏）/ shutdown unsubscribes the same listener instance")
    void shutdownUnsubscribesSameListenerInstance() {
        resolver.start(listener);

        ArgumentCaptor<JQuickGrpcServiceDiscovery.ServiceChangeListener> captor =
                ArgumentCaptor.forClass(JQuickGrpcServiceDiscovery.ServiceChangeListener.class);
        verify(discovery).subscribe(eq("GreeterService"), captor.capture());

        resolver.shutdown();
        verify(discovery).unsubscribe(eq("GreeterService"), same(captor.getValue()));
    }

    @Test
    @DisplayName("shutdown 后变更回调被忽略 / change callbacks after shutdown are ignored")
    void callbacksAfterShutdownAreIgnored() {
        when(discovery.getInstances("GreeterService"))
                .thenReturn(Collections.singletonList(instance("127.0.0.1", 9000)));
        resolver.start(listener);

        ArgumentCaptor<JQuickGrpcServiceDiscovery.ServiceChangeListener> captor =
                ArgumentCaptor.forClass(JQuickGrpcServiceDiscovery.ServiceChangeListener.class);
        verify(discovery).subscribe(eq("GreeterService"), captor.capture());

        resolver.shutdown();
        org.mockito.Mockito.clearInvocations(listener);
        captor.getValue().onChange("GreeterService", Collections.emptyList());

        verifyNoInteractions(listener);
    }
}
