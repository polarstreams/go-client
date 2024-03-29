//go:build integration
// +build integration

package integration

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	. "github.com/polarstreams/go-client"
	"github.com/polarstreams/go-client/types"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	partitionKeyT0Range = "123"
	partitionKeyT1Range = "567"
	partitionKeyT2Range = "234"
)

const discoveryPort = 9250

func Test(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Integration test suite")
}

var _ = Describe("Producer", func ()  {
	expectedBrokers, _ := strconv.Atoi(env("TEST_EXPECTED_BROKERS", "1"))
	if isK8s() {
		expectedBrokers = 3
	}

	const topic = "producer-test"
	host := env("TEST_DISCOVERY_HOST", "polar")

	It("should discover the cluster with port and send data", func ()  {
		producer := newTestProducer(fmt.Sprintf("polar://%s:%d", host, discoveryPort))
		defer producer.Close()
		err := producer.Send(topic, strings.NewReader(`{"hello": 1}`), partitionKeyT0Range)
		Expect(err).NotTo(HaveOccurred())
		Expect(producer.BrokersLength()).To(Equal(expectedBrokers))
	})

	It("should discover the cluster without port and send data", func ()  {
		producer := newTestProducer(fmt.Sprintf("polar://%s", host))
		defer producer.Close()
		err := producer.Send(topic, strings.NewReader(`{"hello": 2}`), partitionKeyT0Range)
		Expect(err).NotTo(HaveOccurred())
		Expect(producer.BrokersLength()).To(Equal(expectedBrokers))
	})

	It("should send multiple requests in parallel", func ()  {
		producer := newTestProducer(fmt.Sprintf("polar://%s", host))
		defer producer.Close()

		const total = 64
		responses := make(chan error, total)

		for i := 0; i < total; i++ {
			go func(v int) {
				responses <- producer.Send(topic, strings.NewReader(fmt.Sprintf(`{"hello": %03d}`, v)), partitionKeyT0Range)
			}(i)
		}

		for i := 0; i < total; i++ {
			Expect(<-responses).NotTo(HaveOccurred())
		}
	})
})

func newTestProducer(serviceUrl string) Producer {
	producer, err := NewProducerWithOpts(serviceUrl, types.ProducerOptions{
		Logger: types.StdLogger,
	})
	Expect(err).NotTo(HaveOccurred())
	return producer
}

func env(key string, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		value = defaultValue
	}
	return value
}

func isK8s() bool {
	return env("KUBERNETES_SERVICE_HOST", "") != ""
}
