package com.github.paohaijiao.grpc.resolver;

import com.github.paohaijiao.grpc.discovery.JQuickGrpcServiceDiscovery;
import com.github.paohaijiao.grpc.domain.JQuickGrpcServiceInstance;
import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import io.grpc.Status;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

public class JQuickGrpcNameResolver extends NameResolver {

    private final String serviceName;

    private final JQuickGrpcServiceDiscovery serviceDiscovery;

    private final Executor executor;

    private Listener2 listener;

    private boolean resolving = false;

    public JQuickGrpcNameResolver(String serviceName, JQuickGrpcServiceDiscovery serviceDiscovery, Executor executor) {
        this.serviceName = serviceName;
        this.serviceDiscovery = serviceDiscovery;
        this.executor = executor;
    }

    @Override
    public String getServiceAuthority() {
        return serviceName;
    }

    @Override
    public void start(Listener2 listener) {  // 使用 Listener2
        this.listener = listener;
        serviceDiscovery.subscribe(serviceName, this::onServiceChange);
        resolve();
    }

    @Override
    public void shutdown() {
        serviceDiscovery.unsubscribe(serviceName, this::onServiceChange);
    }

    @Override
    public void refresh() {
        resolve();
    }

    private void resolve() {
        if (resolving) {
            return;
        }
        resolving = true;
        executor.execute(() -> {
            try {
                List<JQuickGrpcServiceInstance> instances = serviceDiscovery.getInstances(serviceName);
                List<EquivalentAddressGroup> addresses = convertToAddressGroups(instances);
                if (addresses.isEmpty()) {
                    listener.onError(Status.NOT_FOUND.withDescription("No instances found for service: " + serviceName));
                } else {
                    listener.onResult(ResolutionResult.newBuilder()
                            .setAddresses(addresses)
                            .setAttributes(Attributes.EMPTY)
                            .build());
                }
            } catch (Exception e) {
                listener.onError(Status.UNAVAILABLE.withCause(e));
            } finally {
                resolving = false;
            }
        });
    }

    private void onServiceChange(String serviceName, List<JQuickGrpcServiceInstance> instances) {
        resolve();
    }

    private List<EquivalentAddressGroup> convertToAddressGroups(List<JQuickGrpcServiceInstance> instances) {
        return instances.stream()
                .filter(JQuickGrpcServiceInstance::isHealthy)
                .map(instance -> {
                    InetSocketAddress address = new InetSocketAddress(instance.getHost(), instance.getPort());
                    return new EquivalentAddressGroup(address);
                })
                .collect(Collectors.toList());
    }
}