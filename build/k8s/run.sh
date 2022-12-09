# Create PolarStreams resources
kc apply -f build/k8s/polar.yml

# Create test pod
kc apply -f build/k8s/test.yml

# Copy sources to pod
kc cp . test-client:/go/src

# Run integration tests
kc exec test-client -- bash -c "cd src/src && TEST_DISCOVERY_HOST=polar.polar go test -v -count=1 -tags=integration ./internal/test/integration/"
