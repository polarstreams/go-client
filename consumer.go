package polar

import (
	"fmt"
	"os"

	. "github.com/polarstreams/go-client/internal"
	. "github.com/polarstreams/go-client/types"
	"github.com/google/uuid"
)

// Represents a PolarStreams client that reads records from a cluster.
type Consumer interface {
	// Retrieves  data for the topics subscribed.
	// On each poll, consumer will try to use the last consumed offset as the starting offset and fetch sequentially.
	Poll() ConsumerPollResult

	// Gets a point-in-time value of the number of brokers in the cluster
	BrokersLength() int

	// Explicitly reports that the data polled has been consumed to all brokers in the cluster.
	//
	// When exiting the application cleanly, it's usually recommended to invoke ManualCommit() once before Close() to
	// make sure other consumer of the group can continue where this consumer left off.
	//
	// When polling in loop from a consumer, it's usually not necessary to call ManualCommit() per each pool
	// as the broker will automatically commit the consumer offsets periodically.
	ManualCommit() ConsumerCommitResult

	// Closes the consumer
	//
	// A Consumer instance is designed to be long-lived. Close() should only be called when no more messages
	// should be read in the application.
	Close()
}

// NewConsumer creates a new Consumer, discovers the PolarStreams cluster and subscribes to the topic provided.
func NewConsumer(serviceUrl string, consumerGroup string, topic string) (Consumer, error) {
	options := ConsumerOptions{
		Group:  consumerGroup,
		Id:     "",
		Topics: []string{topic},
	}

	return NewConsumerWithOpts(serviceUrl, options)
}

// NewConsumer creates a new Consumer with the provided options.
//
// It discovers the PolarStreams cluster and subscribes to the topics provided.
func NewConsumerWithOpts(serviceUrl string, options ConsumerOptions) (Consumer, error) {
	hostName, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("Host name could not be retrieved to build the consumer id: %s", err)
	}
	if options.Id == "" {
		options.Id = fmt.Sprintf("%s_%s", hostName, uuid.New())
	}
	if err = validateConsumerOptions(&options); err != nil {
		return nil, err
	}

	client, err := NewClient(serviceUrl, fromConsumerOptions(&options))
	if err != nil {
		return nil, err
	}

	if err := client.Connect(); err != nil {
		return nil, err
	}

	client.RegisterAsConsumer(options)

	c := &consumer{
		options: options,
		client:  client,
	}
	return c, nil
}

func validateConsumerOptions(options *ConsumerOptions) error {
	if options.Group == "" {
		return fmt.Errorf("Group can not be empty")
	}
	if len(options.Topics) < 1 {
		return fmt.Errorf("Topics can not be empty")
	}

	if options.Logger == nil {
		options.Logger = NoopLogger
	}

	return nil
}

type consumer struct {
	client  *Client
	options ConsumerOptions
}

func (c *consumer) Poll() ConsumerPollResult {
	return c.client.Poll()
}

func (c *consumer) ManualCommit() ConsumerCommitResult {
	return c.client.ManualCommit()
}

func (p *consumer) BrokersLength() int {
	return p.client.Topology().Length
}

func (c *consumer) Close() {
	c.client.Close()
}

func fromConsumerOptions(options *ConsumerOptions) *ClientOptions {
	return &ClientOptions{
		Logger: options.Logger,
	}
}
