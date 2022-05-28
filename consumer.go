package barco

import (
	"fmt"
	"os"

	. "github.com/barcostreams/go-client/internal"
	. "github.com/barcostreams/go-client/types"
	"github.com/google/uuid"
)

// Represents a Barco client that reads records from a cluster.
type Consumer interface {
	// Retrieves  data for the topics subscribed.
	// On each poll, consumer will try to use the last consumed offset as the starting offset and fetch sequentially.
	Poll() ConsumerPollResult

	// Gets a point-in-time value of the number of brokers in the cluster
	BrokersLength() int

	// Closes the consumer
	//
	// A Consumer instance is designed to be long-lived. Close() should only be called when no more messages
	// should be read in the application.
	Close()
}

// NewConsumer creates a new Consumer, discovers the barco cluster and subscribes to the topic provided.
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
// It discovers the barco cluster and subscribes to the topics provided.
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
