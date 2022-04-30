package internal

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/barcostreams/go-client/internal/utils"
	"github.com/barcostreams/go-client/models"
	"golang.org/x/net/http2"
)

const defaultPollInterval = 10 * time.Second
const baseReconnectionDelay = 100 * time.Millisecond
const maxReconnectionDelay = 2 * time.Minute
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
	producersStatus *utils.CopyOnWriteMap
	logger          models.Logger
	fixedReconnectionDelay time.Duration // To simplify testing
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
	producersStatus := utils.NewCopyOnWriteMap()

	client := &Client{
		discoveryClient: &http.Client{
			Transport: &http.Transport{
				DisableKeepAlives: true, // Disable pooling to target one host each time
				MaxConnsPerHost:   1,
			},
			Timeout: 2 * time.Second,
		},
		producerClient: nil,
		discoveryUrl:  discoveryUrl,
		topology:      atomic.Value{},
		pollInterval:  defaultPollInterval,
		producersStatus: producersStatus,
		isClosing:     0,
		producerIndex: 0,
		logger: models.NoopLogger,
		fixedReconnectionDelay: 0,
	}

	producerClient := &http.Client{
		Transport: &http2.Transport{
			StrictMaxConcurrentStreams: true, // One connection per host
			AllowHTTP:                  true,
			DialTLS: func(network, addr string, cfg *tls.Config) (net.Conn, error) {
				// Pretend we are dialing a TLS endpoint.
				client.logger.Info("Creating new connection to %s", addr)
				conn, err := net.Dial(network, addr)
				if err != nil {
					client.logger.Warn("Connection to %s could not be established", addr)
					client.startReconnecting(addr)
					// There was an error when trying to create the first connection in the pool
					return nil, err
				}

				// There's a possible race condition (a connection closing while a new request is being issued)
				// This should resolve it
				client.getProducerStatus(addr).SetAsUp()

				c := utils.NewTrackedConnection(conn, func(c *utils.TrackedConnection) {
					if atomic.LoadInt64(&client.isClosing) == 1 {
						return
					}
					client.logger.Warn("Connection to %s closed", addr)
					client.startReconnecting(addr)
				})

				return c, nil
			},
			ReadIdleTimeout: 1000 * time.Millisecond,
			PingTimeout:     1000 * time.Millisecond,
		},
	}
	client.producerClient = producerClient

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

	c.topology.Store(topology)
	go c.pollTopology()
	return nil
}

func (c *Client) isProducerUp(ordinal int, t *Topology) bool {
	return c.getProducerStatusByOrdinal(ordinal, t).IsUp()
}

func (c *Client) getProducerStatusByOrdinal(ordinal int, t *Topology) *BrokerStatusInfo {
	key := fmt.Sprintf("%s:%d", t.hostName(ordinal), t.ProducerPort)
	return c.getProducerStatus(key)
}

func (c *Client) getProducerStatus(key string) *BrokerStatusInfo {
	v, _, _ := c.producersStatus.LoadOrStore(key, func() (interface{}, error) {
		return NewBrokerStatusInfo(), nil
	})
	return v.(*BrokerStatusInfo)
}

func (c *Client) startReconnecting(addr string) {
	brokerStatus := c.getProducerStatus(addr)
	if atomic.LoadInt64(&c.isClosing) == 1 {
		return
	}
	if !brokerStatus.SetAsReconnecting() {
		// There's already another thread attempting to reconnect
		return
	}
	brokerStatus.SetAsDown()
	c.logger.Info("Starting reconnection to %s", addr)

	go func() {
		// Use a simple HTTP/1 client to attempt reconnections
		client := &http.Client{
			Timeout: 2 * time.Second,
		}
		defer client.CloseIdleConnections()

		for i := 0; i < 100; i++ {
			if atomic.LoadInt64(&c.isClosing) == 1 {
				return
			}
			if c.fixedReconnectionDelay == 0 {
				delay := maxReconnectionDelay
				if i < 32 {
					delay = time.Duration(1 << i)*baseReconnectionDelay
					if delay > maxReconnectionDelay {
						delay = maxReconnectionDelay
					}
				}
				time.Sleep(jitter(delay))
			} else {
				time.Sleep(c.fixedReconnectionDelay)
			}

			resp, err := client.Get(fmt.Sprintf("http://%s/status", addr))
			defer bodyClose(resp)
			if err == nil && resp.StatusCode == http.StatusOK {
				c.logger.Info("Reconnection attempt to %s succeeded", addr)
				break
			} else {
				c.logger.Warn("Reconnection attempt to %s failed", addr)
			}
		}

		// Either the connection attempt succeeded or we gave up retrying (allow foreground reconnections)
		brokerStatus.SetAsUp()
		brokerStatus.SetAsReconnected()
	}()
}

func (c *Client) Topology() *Topology {
	return c.topology.Load().(*Topology)
}

func (c *Client) pollTopology() {
	for atomic.LoadInt64(&c.isClosing) == 0 {
		time.Sleep(jitter(c.pollInterval))
		newTopology, err := c.queryTopology()
		if err != nil {
			// TODO: Use logging
			continue
		}

		c.topology.Store(newTopology)
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
		ordinal = c.getNextProducerOrdinal(t)
	} else {
		ordinal = c.getNaturalOwner(partitionKey, t)
	}

	maxAttempts := int(math.Min(float64(t.Length), 4))
	for i := 0; i < maxAttempts; i++ {
		brokerOrdinal := (ordinal + i) % t.Length
		if !c.isProducerUp(brokerOrdinal, t) {
			c.logger.Debug("B%d is down, moving to next host", brokerOrdinal)
		}
		url := t.ProducerUrl(topic, brokerOrdinal, partitionKey)
		resp, err := c.producerClient.Post(url, contentType, message)
		if err == nil {
			return resp, nil
		}
	}
	return nil, fmt.Errorf("No broker available: attempted %d brokers", maxAttempts)
}

func (c *Client) Close() {
	c.logger.Info("Barco client closing")
	atomic.StoreInt64(&c.isClosing, 1)
	c.discoveryClient.CloseIdleConnections()
	c.producerClient.CloseIdleConnections()
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
