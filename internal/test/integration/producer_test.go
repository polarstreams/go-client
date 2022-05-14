//go:build integration
// +build integration

package integration

import (
	"strings"
	"testing"

	"github.com/barcostreams/go-client"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	partitionKeyT0Range = "123"
	partitionKeyT1Range = "567"
	partitionKeyT2Range = "234"
)

func Test(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Integration test suite")
}

var _ = Describe("Producer", func ()  {
	It("should work", func ()  {
		producer, err := barco.NewProducer("barco://barco:8083")
		Expect(err).NotTo(HaveOccurred())
		err = producer.Send("abc", strings.NewReader(`{"hello": 1}`), partitionKeyT0Range)
		Expect(err).NotTo(HaveOccurred())
	})
})
