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
	Poll() ConsumerPollResult

	// Gets a point-in-time value of the number of brokers in the cluster
	BrokersLength() int

	// Closes the consumer
	//
	// A Consumer instance is designed to be long-lived. Close() should only be called when no more messages
	// should be read in the application.
	Close()
}

// NewConsumer creates a new Consumer, discovers the barco cluster and subscribes to all the topics.
func NewConsumer(serviceUrl string, consumerGroup string, topic string) (Consumer, error) {
	hostName, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("Host name could not be retrieved to build the consumer id: %s", err)
	}

	client, err := NewClient(serviceUrl)
	if err != nil {
		return nil, err
	}

	id := fmt.Sprintf("%s_%s", hostName, uuid.New())
	options := ConsumerOptions{
		Group:  consumerGroup,
		Id:     id,
		Topics: []string{topic},
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
