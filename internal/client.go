package internal

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	"golang.org/x/net/http2"
)

const defaultPollInterval = 10 * time.Second
const discoveryPath = "/v1/brokers"
const maxOrdinal = 1 << 31

type Client struct {
	discoveryClient *http.Client
	producerClient  *http.Client
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
				MaxConnsPerHost:   1,
			},
			Timeout: 2 * time.Second,
		},
		producerClient: &http.Client{
			Transport: &http2.Transport{
				StrictMaxConcurrentStreams: true, // One connection per host
				AllowHTTP:                  true,
				DialTLS: func(network, addr string, cfg *tls.Config) (net.Conn, error) {
					// Pretend we are dialing a TLS endpoint.
					log.Printf("Creating new connection")
					return net.Dial(network, addr)
				},
				ReadIdleTimeout: 1000 * time.Millisecond,
				PingTimeout:     1000 * time.Millisecond,
			},
		},
		discoveryUrl:  discoveryUrl,
		topology:      atomic.Value{},
		pollInterval:  defaultPollInterval,
		isClosing:     0,
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

func (c *Client) ProduceJson(topic string, message io.Reader, partitionKey string) (*http.Response, error) {
	t := c.Topology()
	ordinal := 0
	if partitionKey == "" {
		ordinal = c.getNextProducerOrdinal(&t)
	} else {
		ordinal = c.getNaturalOwner(partitionKey, &t)
	}

	url := t.ProducerUrl(topic, ordinal, partitionKey)
	return c.producerClient.Post(url, contentType, message)
}

func (c *Client) Close() {

}

func bodyClose(r *http.Response) {
	if r != nil && r.Body != nil {
		r.Body.Close()
	}
}

func (c *Client) getNextProducerOrdinal(t *Topology) int {
	value := atomic.AddUint32(&c.producerIndex, 1)
	if value >= maxOrdinal {
		// Atomic inc operations don't wrap around.
		// Not exactly fair when value >= maxOrdinal, but in practical terms is good enough
		atomic.StoreUint32(&c.producerIndex, 0)
	}
	return (int(value) - 1) % t.Length
}

func (c *Client) getNaturalOwner(partitionKey string, t *Topology) int {
	return PrimaryBroker(partitionKey, t.Length)
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
