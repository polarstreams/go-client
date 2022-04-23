package internal

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"
)

const defaultPollInterval = 10 * time.Second
const discoveryPath = "/v1/brokers"

type Client struct {
	discoveryClient *http.Client
	discoveryUrl    string
	topology        atomic.Value
	isClosing       int64
	pollInterval    time.Duration
	producerIndex   uint32
}

var jitterRng = rand.New(rand.NewSource(time.Now().UnixNano()))

func NewClient(serviceUrl string) (*Client, error) {
	u, err := url.Parse(serviceUrl)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "barco" {
		return nil, fmt.Errorf("Invalid scheme: %s, expected 'barco'", u.Scheme)
	}

	path := discoveryPath

	if u.Path != "/" && u.Path != "" {
		path = u.Path
	}

	discoveryUrl := fmt.Sprintf("http://%s%s", u.Host, path)

	client := &Client{
		discoveryClient: &http.Client{
			Transport: &http.Transport{
				DisableKeepAlives: true, // Disable pooling to target one host each time
				MaxConnsPerHost: 1,
			},
			Timeout: 2 * time.Second,
		},
		discoveryUrl: discoveryUrl,
		topology: atomic.Value{},
		pollInterval: defaultPollInterval,
		isClosing: 0,
		producerIndex: 0,
	}

	return client, nil
}

// Gets the topology the first time and starts the loop for polling for changes.
//
// Close() should be called to stop polling.
func (c *Client) Connect() error {
	topology, err := c.queryTopology()
	if err != nil {
		return err
	}

	c.topology.Store(*topology)
	go c.pollTopology()
	return nil
}

func (c *Client) Topology() Topology {
	return c.topology.Load().(Topology)
}

func (c *Client) pollTopology() {
	for atomic.LoadInt64(&c.isClosing) == 0 {
		time.Sleep(jitter(c.pollInterval))
		newTopology, err := c.queryTopology()
		if err != nil {
			// TODO: Use logging
			continue
		}

		c.topology.Store(*newTopology)
	}
}

func (c *Client) queryTopology() (*Topology, error) {
	resp, err := c.discoveryClient.Get(c.discoveryUrl)
	if err != nil {
		return nil, err
	}
	defer bodyClose(resp)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("The discovery service replied with error code: %s", resp.Status)
	}

	value := Topology{}
	if err = json.NewDecoder(resp.Body).Decode(&value); err != nil {
		return nil, err
	}

	return &value, nil
}

func (c *Client) ProduceJson(topic string, message io.Reader, partitionKey string) {
	// func (c *TestClient) ProduceJson(ordinal int, topic string, message string, partitionKey string) *http.Response {
	// 	url := c.ProducerUrl(ordinal, topic, partitionKey)
	// 	resp, err := c.client.Post(url, "application/json", strings.NewReader(message))
}

func (c *Client) Close() {

}

func bodyClose(r *http.Response) {
	if r != nil && r.Body != nil {
		r.Body.Close()
	}
}

// Adds a +-5% jitter to the duration with millisecond resolution
func jitter(t time.Duration) time.Duration {
	ms := float64(t.Milliseconds())
	maxJitter := 0.1 * ms
	if maxJitter < 1 {
		panic("Delay should be at least 20ms")
	}
	jitterRange := jitterRng.Float64() * maxJitter
	startJitter := 0.05 * ms
	return time.Duration(ms-startJitter+jitterRange) * time.Millisecond
}
