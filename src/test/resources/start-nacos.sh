#!/bin/bash
echo "Starting Nacos with authentication for testing..."
if docker ps -a --format '{{.Names}}' | grep -q "^nacos-test$"; then
    echo "Removing existing nacos-test container..."
    docker rm -f nacos-test
fi
docker run -d \
  --name nacos-test \
  -p 8848:8848 \
  -p 9848:9848 \
  -p 9849:9849 \
  -e MODE=standalone \
  -e PREFER_HOST_MODE=hostname \
  -e NACOS_AUTH_ENABLE=true \
  -e NACOS_AUTH_IDENTITY_KEY=serverIdentity \
  -e NACOS_AUTH_IDENTITY_VALUE=security \
  -e NACOS_AUTH_TOKEN=SecretKey012345678901234567890123456789012345678901234567890123456789 \
  nacos/nacos-server:v2.3.0

echo "Waiting for Nacos to start..."
sleep 15
echo "Creating user paohaijiao..."
docker exec nacos-test bash -c "
curl -X POST 'http://localhost:8848/nacos/v1/auth/users' \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'username=paohaijiao&password=1qaz@WSX'
" 2>/dev/null
echo "✓ Nacos is running with authentication"
echo "  Console: http://localhost:8848/nacos"
echo "  Username: paohaijiao"
echo "  Password: 1qaz@WSX"
echo ""
echo "Run tests with: mvn test -Dtest=JQuickGrpcNacosDiscoveryTest"