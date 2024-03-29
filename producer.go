package polar

import (
	"io"

	. "github.com/polarstreams/go-client/internal"
	"github.com/polarstreams/go-client/types"
)

// Represents a PolarStreams client that publishes records in a cluster.
type Producer interface {
	// Sends a message to a topic
	Send(topic string, message io.Reader, partitionKey string) error

	// Gets a point-in-time value of the number of brokers in the cluster
	BrokersLength() int

	// Closes the producer
	//
	// A Producer instance is designed to be long-lived. Close() should only be called when no more messages are
	// expected to be published in the cluster.
	Close()
}

// NewProducer creates a new Producer and discovers the PolarStreams cluster.
//
// A Producer instance is designed to be long-lived and it should be reused across the application.
func NewProducer(serviceUrl string) (Producer, error) {
	return NewProducerWithOpts(serviceUrl, types.ProducerOptions{
		Logger: types.NoopLogger,
	})
}

func fromProducerOptions(o *types.ProducerOptions) *ClientOptions {
	return &ClientOptions{
		Logger:                      o.Logger,
		ProducerInitialize:          true,
		ProducerFlushThresholdBytes: o.FlushThresholdBytes,
		ProducerConnectionsPerHost:  o.ConnectionsPerBroker,
	}
}

// NewProducer creates a new Producer with the provided options and discovers the PolarStreams cluster.
//
// A Producer instance is designed to be long-lived and it should be reused across the application.
func NewProducerWithOpts(serviceUrl string, options types.ProducerOptions) (Producer, error) {
	client, err := NewClient(serviceUrl, fromProducerOptions(&options))
	if err != nil {
		return nil, err
	}

	return &producer{
		client: client,
	}, nil
}

type producer struct {
	client *Client
}

func (p *producer) Send(topic string, message io.Reader, partitionKey string) error {
	return p.client.ProduceJson(topic, message, partitionKey)
}

func (p *producer) BrokersLength() int {
	return p.client.Topology().Length
}

func (p *producer) Close() {
	p.client.Close()
}
