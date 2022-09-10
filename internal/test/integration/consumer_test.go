//go:build integration
// +build integration

package integration

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	. "github.com/barcostreams/go-client"
	"github.com/barcostreams/go-client/types"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const maxPollInterval = 2 * time.Second
const timerPrecision = 50 * time.Millisecond

var _ = Describe("Consumer", func ()  {
	expectedBrokers, _ := strconv.Atoi(env("TEST_EXPECTED_BROKERS", "1"))
	if isK8s() {
		expectedBrokers = 3
	}

	host := env("TEST_DISCOVERY_HOST", "barco")
	serviceUrl := fmt.Sprintf("barco://%s", host)

	It("should discover the cluster without port and retrieve data", func ()  {
		topic := fmt.Sprintf("consumer-test-%d", time.Now().UnixMicro())
		group := fmt.Sprintf("g%d", time.Now().UnixMicro())
		p := newTestProducer(serviceUrl)
		defer p.Close()

		messages := []string {
			`{"hello": "B0_fixed"}`,
			`{"hello": "B1_fixed"}`,
			`{"hello": "B2_fixed"}`,
		}

		produce(p, topic, messages[0], partitionKeyT0Range)
		produce(p, topic, messages[1], partitionKeyT1Range)
		produce(p, topic, messages[2], partitionKeyT2Range)

		consumer := newTestConsumer(serviceUrl, types.ConsumerOptions{
			Group:  group,
			Topics: []string{topic},
		})
		defer consumer.Close()
		Expect(consumer.BrokersLength()).To(Equal(expectedBrokers))

		topicRecords := pollUntil(consumer, 3)
		Expect(topicRecords).To(HaveLen(len(messages)))
		for _, m := range messages {
			r, _ := findRecord(topicRecords, m)
			Expect(r).NotTo(BeNil(), "Message '%s' not found", m)
		}

		// Next polls should just wait
		start := time.Now()
		result := consumer.Poll()
		Expect(result.Error).NotTo(HaveOccurred())
		Expect(result.TopicRecords).To(HaveLen(0))
		Expect(time.Since(start)).To(BeNumerically(">=", maxPollInterval - timerPrecision))

		messages = make([]string, 0)
		for i := 0; i < 12; i++ {
			m := fmt.Sprintf(`{"key": "key%d"}`, i)
			produce(p, topic, m, "")
			messages = append(messages, m)
		}

		topicRecords = pollUntil(consumer, 10)
		Expect(topicRecords).To(HaveLen(len(messages)))
		for _, m := range messages {
			r, _ := findRecord(topicRecords, m)
			Expect(r).NotTo(BeNil(), "Message '%s' not found", m)
		}
	})

	Context("Without manual commit", func() {
		It("should rewind to last commit when coming from another connection", func ()  {
			topic := fmt.Sprintf("test-no-commit-%d", time.Now().UnixMicro())
			group := fmt.Sprintf("gc_%d", time.Now().UnixMicro())
			p := newTestProducer(serviceUrl)
			defer p.Close()

			consumer1_conn1 := newTestConsumer(serviceUrl, types.ConsumerOptions{
				Id: 	"consumer1",
				Group:  group,
				Topics: []string{topic},
			})
			defer consumer1_conn1.Close()
			Expect(consumer1_conn1.BrokersLength()).To(Equal(expectedBrokers))

			messages := make([]string, 0)
			for i := 0; i < 12; i++ {
				m := fmt.Sprintf(`{"key": "key%d"}`, i)
				produce(p, topic, m, partitionKeyT0Range)
				messages = append(messages, m)
			}

			topicRecords := pollUntil(consumer1_conn1, 1)
			Expect(len(topicRecords)).To(BeNumerically(">", 0))
			Expect(string(topicRecords[0].Records[0].Body)).To(Equal(`{"key": "key0"}`))

			consumer1_conn2 := newTestConsumer(serviceUrl, types.ConsumerOptions{
				Id: 	"consumer1",
				Group:  group,
				Topics: []string{topic},
			})
			defer consumer1_conn2.Close()

			topicRecords = pollUntil(consumer1_conn2, 1)
			Expect(len(messages)).To(BeNumerically(">", 0))
			Expect(string(topicRecords[0].Records[0].Body)).To(Equal(`{"key": "key0"}`))
		})
	})

	Context("With manual commit", func() {
		It("should continue from committed when coming from another connection", func ()  {
			topic := fmt.Sprintf("test-commit-%d", time.Now().UnixMicro())
			group := fmt.Sprintf("gmc_%d", time.Now().UnixMicro())
			p := newTestProducer(serviceUrl)
			defer p.Close()

			consumer1_conn1 := newTestConsumer(serviceUrl, types.ConsumerOptions{
				Id: 	"consumer1",
				Group:  group,
				Topics: []string{topic},
			})
			defer consumer1_conn1.Close()
			Expect(consumer1_conn1.BrokersLength()).To(Equal(expectedBrokers))

			messages := make([]string, 0)
			for i := 0; i < 12; i++ {
				m := fmt.Sprintf(`{"key": %d}`, i)
				produce(p, topic, m, partitionKeyT0Range)
				messages = append(messages, m)
			}

			topicRecords := pollUntil(consumer1_conn1, 1)
			Expect(len(topicRecords)).To(BeNumerically(">", 0))
			Expect(string(topicRecords[0].Records[0].Body)).To(Equal(`{"key": 0}`))


			// Store the offset in the brokers
			commitResult := consumer1_conn1.ManualCommit()
			Expect(commitResult.Errors).To(BeEmpty())
			Expect(commitResult.SuccessCount).To(Equal(consumer1_conn1.BrokersLength()))

			consumer1_conn1.Close()

			consumer1_conn2 := newTestConsumer(serviceUrl, types.ConsumerOptions{
				Id: 	"consumer1",
				Group:  group,
				Topics: []string{topic},
			})
			defer consumer1_conn2.Close()

			topicRecords = pollUntil(consumer1_conn2, 1)
			Expect(len(messages)).To(BeNumerically(">", 0))

			// Assert that it continued where it left off
			Expect(string(topicRecords[0].Records[0].Body)).To(Equal(fmt.Sprintf(`{"key": %d}`, len(topicRecords))))
		})
	})
})

func produce(producer Producer, topic string, message string, partitionKey string) {
	err := producer.Send(topic, strings.NewReader(message), partitionKey)
	Expect(err).NotTo(HaveOccurred())
}

func newTestConsumer(serviceUrl string, options types.ConsumerOptions) Consumer {
	if options.Logger == nil {
		options.Logger = types.StdLogger
	}
	if options.MaxPollInterval == 0 {
		options.MaxPollInterval = maxPollInterval
	}
	consumer, err := NewConsumerWithOpts(serviceUrl, options)
	Expect(err).NotTo(HaveOccurred())
	return consumer
}

func findRecord(records []types.TopicRecords, message string) (*types.Record, *types.TopicDataId) {
	for _, tr := range records {
		for _, r := range tr.Records {
			if string(r.Body) == message {
				return &r, tr.Topic
			}
		}
	}
	return nil, nil
}

// Polls few times until the amount of message is what expected as
// there's no guarantee that a single poll call will retrieve all messages
func pollUntil(consumer Consumer, length int) []types.TopicRecords {
	topicRecords := make([]types.TopicRecords, 0)
	for i := 0; i < 10; i++ {
		result := consumer.Poll()
		Expect(result.Error).NotTo(HaveOccurred())
		topicRecords = append(topicRecords, result.TopicRecords...)
		if len(topicRecords) >= length {
			break
		}
	}
	return topicRecords
}