package internal

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/barcostreams/go-client/internal/serialization"
	"github.com/barcostreams/go-client/internal/utils"
	. "github.com/barcostreams/go-client/types"
	"golang.org/x/net/context"
	"golang.org/x/net/http2"
)

const topologyPollInterval = 10 * time.Second
const defaultPollReqInterval = 5 * time.Second
const baseReconnectionDelay = 100 * time.Millisecond
const maxReconnectionDelay = 2 * time.Minute
const maxOrdinal = 1 << 31

const (
	discoveryUrl        = "/v1/brokers"
	consumerRegisterUrl = "/v1/consumer/register"
	consumerPollUrl     = "/v1/consumer/poll"
)

type Client struct {
	discoveryClient        *http.Client
	producerClient         *http.Client
	consumerClient         *http.Client
	discoveryUrl           string // The full discovery url, like http://host:port/path
	discoveryHost          string // The host and port
	topology               atomic.Value
	isClosing              int64
	isRegistering          int64
	topologyPollInterval   time.Duration
	producerIndex          uint32
	consumerIndex          uint32
	producersStatus        *utils.CopyOnWriteMap
	consumerStatus         *utils.CopyOnWriteMap
	logger                 Logger
	consumerOptions        ConsumerOptions
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

	path := discoveryUrl

	if u.Path != "/" && u.Path != "" {
		path = u.Path
	}

	discoveryUrl := fmt.Sprintf("http://%s%s", u.Host, path)
	discoveryHost := u.Host

	client := &Client{
		discoveryClient: &http.Client{
			Transport: &http.Transport{
				DisableKeepAlives: true, // Disable pooling to target one host each time
				MaxConnsPerHost:   1,
			},
			Timeout: 2 * time.Second,
		},
		discoveryUrl:         discoveryUrl,
		discoveryHost:        discoveryHost,
		topology:             atomic.Value{},
		topologyPollInterval: topologyPollInterval,
		producersStatus:      utils.NewCopyOnWriteMap(),
		consumerStatus:       utils.NewCopyOnWriteMap(),
		logger:               NoopLogger,
	}

	client.producerClient = &http.Client{
		Transport: &http2.Transport{
			StrictMaxConcurrentStreams: true, // One connection per host
			AllowHTTP:                  true,
			DialTLS: func(network, addr string, cfg *tls.Config) (net.Conn, error) {
				// Pretend we are dialing a TLS endpoint.
				return client.dial(network, addr, client.getProducerStatus(addr))
			},
			ReadIdleTimeout: 1000 * time.Millisecond,
			PingTimeout:     1000 * time.Millisecond,
		},
	}

	client.consumerClient = &http.Client{
		Transport: &http2.Transport{
			StrictMaxConcurrentStreams: true, // One connection per host
			AllowHTTP:                  true,
			DialTLS: func(network, addr string, cfg *tls.Config) (net.Conn, error) {
				conn, err := client.dial(network, addr, client.getConsumerStatus(addr))
				if err == nil && atomic.LoadInt64(&client.isRegistering) == 0 {
					// Register consumer in the background, it might cause the first poll to be ignored but it's OK
					go client.registerConsumerConn(addr)
				}
				return conn, err
			},
			ReadIdleTimeout: 1000 * time.Millisecond,
			PingTimeout:     1000 * time.Millisecond,
		},
	}

	return client, nil
}

func (c *Client) dial(network string, addr string, brokerStatus *BrokerStatusInfo) (*utils.TrackedConnection, error) {
	c.logger.Info("Creating new connection to %s", addr)
	conn, err := net.Dial(network, addr)
	if err != nil {
		c.logger.Warn("Connection to %s could not be established", addr)
		c.startReconnection(addr, brokerStatus)
		// There was an error when trying to create the first connection in the pool
		return nil, err
	}

	// There's a possible race condition (a connection closing while a new request is being issued)
	// This should resolve it
	brokerStatus.SetAsUp()

	tc := utils.NewTrackedConnection(conn, func(_ *utils.TrackedConnection) {
		if atomic.LoadInt64(&c.isClosing) == 1 {
			return
		}
		c.logger.Warn("Connection to %s closed", addr)
		c.startReconnection(addr, brokerStatus)
	})

	return tc, nil
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

func (c *Client) isConsumerUp(ordinal int, t *Topology) bool {
	return c.getConsumerStatusByOrdinal(ordinal, t).IsUp()
}

func (c *Client) getConsumerStatusByOrdinal(ordinal int, t *Topology) *BrokerStatusInfo {
	key := fmt.Sprintf("%s:%d", t.hostName(ordinal), t.ConsumerPort)
	return c.getConsumerStatus(key)
}

func (c *Client) getConsumerStatus(key string) *BrokerStatusInfo {
	v, _, _ := c.consumerStatus.LoadOrStore(key, func() (interface{}, error) {
		return NewBrokerStatusInfo(), nil
	})
	return v.(*BrokerStatusInfo)
}

func (c *Client) registerConsumerConn(addr string) {
	url := fmt.Sprintf("http://%s%s", addr, consumerRegisterUrl)
	jsonBody, err := json.Marshal(serialization.RegisterConsumerInfo{
		Id:     c.consumerOptions.Id,
		Group:  c.consumerOptions.Group,
		Topics: c.consumerOptions.Topics,
	})
	utils.PanicIfErr(err)

	resp, err := c.consumerClient.Post(url, contentType, bytes.NewReader(jsonBody))
	if err != nil {
		// It will retry automatically
		c.logger.Warn("Error while trying to register as consumer: %s", err)
		return
	}

	if resp.StatusCode != http.StatusOK {
		// This is worrisome: there's a HTTP/2 server but the request to register didn't succeeded
		c.logger.Error("Unexpected status code from server: %d", resp.StatusCode)
	}
	defer resp.Body.Close()
}

func (c *Client) startReconnection(addr string, brokerStatus *BrokerStatusInfo) {
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
					delay = time.Duration(1<<i) * baseReconnectionDelay
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
		time.Sleep(jitter(c.topologyPollInterval))
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

	if value.Length == 1 && len(value.BrokerNames) == 1 {
		// Use the discovery host for dev mode
		value = c.useDiscoveryHostForBroker(&value)
	}

	return &value, nil
}

func (c *Client) useDiscoveryHostForBroker(t *Topology) Topology {
	name := c.discoveryHost
	index := strings.LastIndex(name, ":")

	if index > 0 {
		name = name[:index]
	}

	return Topology{
		Length:       1,
		BrokerNames:  []string{name},
		ProducerPort: t.ProducerPort,
		ConsumerPort: t.ConsumerPort,
	}
}

func (c *Client) ProduceJson(topic string, message io.Reader, partitionKey string) (*http.Response, error) {
	t := c.Topology()
	ordinal := 0
	if partitionKey == "" {
		ordinal = c.getNextOrdinal(&c.producerIndex, t)
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

func (c *Client) RegisterAsConsumer(options ConsumerOptions) {
	if options.MaxPollInterval == 0 {
		options.MaxPollInterval = defaultPollReqInterval
	}
	c.consumerOptions = options
	atomic.StoreInt64(&c.isRegistering, 1)
	topology := c.Topology()
	var wg sync.WaitGroup
	for i := 0; i < topology.Length; i++ {
		wg.Add(1)
		addr := fmt.Sprintf("%s:%d", topology.hostName(i), topology.ConsumerPort)
		go func() {
			defer wg.Done()
			c.registerConsumerConn(addr)
		}()
	}

	wg.Wait()
	atomic.StoreInt64(&c.isRegistering, 0)
}

func (c *Client) SendStatusRequestToConsumer() {
	topology := c.Topology()
	var wg sync.WaitGroup
	for i := 0; i < topology.Length; i++ {
		wg.Add(1)
		addr := fmt.Sprintf("%s:%d", topology.hostName(i), topology.ConsumerPort)
		go func() {
			defer wg.Done()
			_, _ = c.consumerClient.Get(fmt.Sprintf("http://%s/status", addr))
		}()
	}

	wg.Wait()
}

func (c *Client) Poll() ConsumerPollResult {
	// This is a trivial implementation, it can be more effective by parallelizing the requests
	// And avoid hitting the brokers that don't yield data for long time
	t := c.Topology()
	start := time.Now()
	maxPollInterval := c.consumerOptions.MaxPollInterval
	ctxt, _ := context.WithDeadline(context.Background(), start.Add(maxPollInterval))
	errors := make([]error, 0)
	iterationDelay := 500 * time.Millisecond

	for i := 0; time.Since(start) < maxPollInterval; i++ {
		if i > 0 && i%t.Length == 0 {
			if len(errors) == i {
				// All hosts tried
				return ConsumerPollResult{
					Error: fmt.Errorf("All brokers polled resulted in error, first error: %s", errors[0]),
				}
			}
		}

		if i >= t.Length {
			factor := 1
			if i >= t.Length*2 {
				factor = 2
			}
			time.Sleep(jitter(iterationDelay * time.Duration(factor)))
		}

		ordinal := c.getNextOrdinal(&c.consumerIndex, t)
		url := fmt.Sprintf("http://%s:%d%s", t.hostName(ordinal), t.ConsumerPort, consumerPollUrl)
		r, err := http.NewRequestWithContext(ctxt, "GET", url, nil)
		utils.PanicIfErr(err)
		resp, err := c.consumerClient.Do(r)
		if err == nil {
			if resp.StatusCode == http.StatusOK {
				topicRecords, err := serialization.ReadOkResponse(resp)
				if err != nil {
					// There was a serialization issue
					return ConsumerPollResult{Error: err}
				}
				if len(topicRecords) == 0 {
					// Kind of an edge case or at least it only happens with mocked data
					continue
				}
				return ConsumerPollResult{TopicRecords: topicRecords}
			}

			if resp.StatusCode == http.StatusNoContent {
				resp.Body.Close()
			} else {
				errors = append(errors, serialization.ReadErrorResponse(resp))
			}
			continue
		}
		errors = append(errors, err)
	}
	return ConsumerPollResult{}
}

func (c *Client) Close() {
	c.logger.Info("Barco client closing")
	atomic.StoreInt64(&c.isClosing, 1)
	c.discoveryClient.CloseIdleConnections()
	c.producerClient.CloseIdleConnections()
	c.consumerClient.CloseIdleConnections()
}

func bodyClose(r *http.Response) {
	if r != nil && r.Body != nil {
		r.Body.Close()
	}
}

func (c *Client) getNextOrdinal(index *uint32, t *Topology) int {
	value := atomic.AddUint32(index, 1)
	if value >= maxOrdinal {
		// Atomic inc operations don't wrap around.
		// Not exactly fair when value >= maxOrdinal, but in practical terms is good enough
		atomic.StoreUint32(index, 0)
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
