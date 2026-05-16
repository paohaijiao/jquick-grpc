#!/bin/bash
echo "Starting Etcd for testing..."
if docker ps -a --format '{{.Names}}' | grep -q "^etcd-test$"; then
    echo "Removing existing etcd-test container..."
    docker rm -f etcd-test
fi
docker run -d \
  --name etcd-test \
  -p 2379:2379 \
  -p 2380:2380 \
  -e ALLOW_NONE_AUTHENTICATION=yes \
  -e ETCD_ADVERTISE_CLIENT_URLS=http://0.0.0.0:2379 \
  -e ETCD_LISTEN_CLIENT_URLS=http://0.0.0.0:2379 \
  bitnami/etcd:3.5.9
echo "Waiting for Etcd to start..."
sleep 5
if docker exec etcd-test etcdctl endpoint health 2>/dev/null | grep -q "healthy"; then
    echo "✓ Etcd is running on http://localhost:2379"
    echo "Run tests with: mvn test -Dtest=JQuickGrpcEtcdDiscoveryTest"
else
    echo "✗ Etcd may not be healthy, checking logs..."
    docker logs etcd-test --tail 20
fi