package barco

import (
	"fmt"
	"io"
	"net/http"

	. "github.com/barcostreams/go-client/internal"
	"github.com/barcostreams/go-client/internal/utils"
)

// Represents a Barco client that publishes records in a cluster.
type Producer interface {
	// Sends a message to a topic
	Send(topic string, message io.Reader, partitionKey string) error

	// Closes the producer
	//
	// A Producer instance is designed to be long-lived. Close() should only be called when no more messages are
	// expected to be published in the cluster.
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

func (p *producer) Send(topic string, message io.Reader, partitionKey string) error {
	resp, err := p.client.ProduceJson(topic, message, partitionKey)
	if err != nil {
		return err
	}
	if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
		defer resp.Body.Close()
		return nil
	}
	body, err := utils.ReadBody(resp)
	if err != nil {
		return err
	}
	return fmt.Errorf(body)
}

func (p *producer) Close() {
	p.client.Close()
}
