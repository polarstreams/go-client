package barco

import (
	. "github.com/barcostreams/go-client/internal"
)

type Producer interface {
	Send(partitionKey string, data []byte) error

	Close()
}

// NewProducer creates a new Producer and discovers the barco cluster.
//
// A Producer instance is designed to be long-lived and it should be reused across the application.
func NewProducer(serviceUrl string) (Producer, error) {
	client, err := NewClient(serviceUrl)
	if err != nil {
		return nil, err
	}

	if err := client.Connect(); err != nil {
		return nil, err
	}

	return &producer{
		client: client,
	}, nil
}

type producer struct {
	client *Client
}

func (p *producer) Send(partitionKey string, data []byte) error {
	return nil
}

func (p *producer) Close() {
	p.client.Close()
}
