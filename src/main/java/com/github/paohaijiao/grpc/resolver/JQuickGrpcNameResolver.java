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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * <p>
 * JQuick discovery-backed NameResolver. Fixes: (1) subscribe/unsubscribe now share one
 * listener instance (a method reference creates a new object per evaluation, so the old
 * code could never unsubscribe); (2) re-entrant resolves are guarded atomically;
 * (3) refresh/callbacks before start are ignored instead of NPE.
 */
public class JQuickGrpcNameResolver extends NameResolver {

    private final String serviceName;

    private final JQuickGrpcServiceDiscovery serviceDiscovery;

    private final Executor executor;

    /**
     * The single listener instance shared by start/shutdown for correct unsubscription.
     */
    private final JQuickGrpcServiceDiscovery.ServiceChangeListener changeListener = this::onServiceChange;

    private final AtomicBoolean resolving = new AtomicBoolean(false);

    private volatile Listener2 listener;

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
    public void start(Listener2 listener) {
        this.listener = listener;
        serviceDiscovery.subscribe(serviceName, changeListener);
        resolve();
    }

    @Override
    public void shutdown() {
        listener = null;
        serviceDiscovery.unsubscribe(serviceName, changeListener);
    }

    @Override
    public void refresh() {
        resolve();
    }

    private void resolve() {
        Listener2 current = listener;
        if (current == null) {
            // not started yet or already shut down
            return;
        }
        if (!resolving.compareAndSet(false, true)) {
            return;
        }
        executor.execute(() -> {
            try {
                List<JQuickGrpcServiceInstance> instances = serviceDiscovery.getInstances(serviceName);
                List<EquivalentAddressGroup> addresses = convertToAddressGroups(instances);
                if (addresses.isEmpty()) {
                    current.onError(Status.NOT_FOUND.withDescription("No instances found for service: " + serviceName));
                } else {
                    current.onResult(ResolutionResult.newBuilder()
                            .setAddresses(addresses)
                            .setAttributes(Attributes.EMPTY)
                            .build());
                }
            } catch (Exception e) {
                current.onError(Status.UNAVAILABLE.withCause(e));
            } finally {
                resolving.set(false);
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
