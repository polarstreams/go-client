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
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/polarstreams/go-client/internal/serialization"
	"github.com/polarstreams/go-client/internal/serialization/producer"
	"github.com/polarstreams/go-client/internal/utils"
	. "github.com/polarstreams/go-client/types"
	"golang.org/x/net/context"
	"golang.org/x/net/http2"
)

const DefaultTopologyPollInterval = 10 * time.Second
const defaultPollReqInterval = 5 * time.Second
const baseReconnectionDelay = 100 * time.Millisecond
const maxReconnectionDelay = 2 * time.Minute
const maxAtomicIncrement = 1 << 31
const defaultDiscoveryPort = 9250

const (
	discoveryUrl            = "/v1/brokers"
	consumerRegisterUrl     = "/v1/consumer/register"
	consumerPollUrl         = "/v1/consumer/poll"
	consumerManualCommitUrl = "/v1/consumer/commit"
)

type Client struct {
	discoveryClient        *http.Client
	producerClient         *producerClient
	consumerClient         *http.Client
	discoveryUrl           string // The full discovery url, like http://host:port/path
	discoveryHost          string // The host and port
	topology               atomic.Value
	isClosing              int64
	isRegistering          int64
	topologyPollInterval   time.Duration
	producerIndex          uint32
	producersStatus        *utils.CopyOnWriteMap
	consumerStatus         *utils.CopyOnWriteMap
	lastConsumerError      int64
	logger                 Logger
	consumerOptions        ConsumerOptions
	fixedReconnectionDelay time.Duration // To simplify testing
}

type ClientOptions struct {
	Logger                      Logger
	TopologyPollInterval        time.Duration
	FixedReconnectionDelay      time.Duration
	ProducerInitialize          bool
	ProducerFlushThresholdBytes int
	ProducerConnectionsPerHost  int
}

var jitterRng = rand.New(rand.NewSource(time.Now().UnixNano()))

func NewClient(serviceUrl string, options *ClientOptions) (*Client, error) {
	u, err := url.Parse(serviceUrl)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "polar" {
		return nil, fmt.Errorf("Invalid scheme: %s, expected 'polar'", u.Scheme)
	}
	if options == nil {
		options = &ClientOptions{}
	}
	setDefaultOptions(options)

	path := discoveryUrl

	if u.Path != "/" && u.Path != "" {
		path = u.Path
	}

	host := u.Host
	if !strings.Contains(host, ":") {
		host = fmt.Sprintf("%s:%d", host, defaultDiscoveryPort)
	}

	discoveryUrl := fmt.Sprintf("http://%s%s", host, path)
	discoveryHost := host

	client := &Client{
		discoveryClient: &http.Client{
			Transport: &http.Transport{
				DisableKeepAlives: true, // Disable pooling to target one host each time
				MaxConnsPerHost:   1,
			},
			Timeout: 2 * time.Second,
		},
		discoveryUrl:           discoveryUrl,
		discoveryHost:          discoveryHost,
		topology:               atomic.Value{},
		topologyPollInterval:   options.TopologyPollInterval,
		producersStatus:        utils.NewCopyOnWriteMap(),
		consumerStatus:         utils.NewCopyOnWriteMap(),
		logger:                 options.Logger,
		fixedReconnectionDelay: options.FixedReconnectionDelay,
	}

	topology, err := client.queryTopology()
	if err != nil {
		return nil, err
	}
	options.Logger.Info("Discovered cluster composed of %d brokers", topology.Length)

	if topology.ProducerBinaryPort == 0 {
		return nil, fmt.Errorf("Invalid server version, make sure you use the latest polar version")
	}

	client.topology.Store(topology)
	go client.pollTopology()

	if options.ProducerInitialize {
		// The producer client eagerly creates connections
		client.producerClient = newProducerClient(
			topology,
			options.ProducerConnectionsPerHost,
			options.ProducerFlushThresholdBytes,
			options.FixedReconnectionDelay,
			options.Logger)
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
	c.logger.Info("Creating new consumer connection to %s", addr)
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

//nolint:golint,unused
func (c *Client) isProducerUp(ordinal int) bool {
	return c.producerClient.IsProducerUp(ordinal)
}

//nolint:golint,unused
func (c *Client) getProducerStatusByOrdinal(ordinal int, t *Topology) *BrokerStatusInfo {
	key := fmt.Sprintf("%s:%d", t.hostName(ordinal), t.ProducerPort)
	return c.getProducerStatus(key)
}

//nolint:golint,unused
func (c *Client) getProducerStatus(key string) *BrokerStatusInfo {
	v, _, _ := c.producersStatus.LoadOrStore(key, func() (interface{}, error) {
		return NewBrokerStatusInfo(), nil
	})
	return v.(*BrokerStatusInfo)
}

//nolint:golint,unused
func (c *Client) isConsumerUp(ordinal int, t *Topology) bool {
	return c.getConsumerStatusByOrdinal(ordinal, t).IsUp()
}

//nolint:golint,unused
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
			c.logger.Warn("Error while trying to get the topology: %s", err)
			continue
		}
		currentTopology := c.Topology()
		if !reflect.DeepEqual(*newTopology, *currentTopology) {
			c.topology.Store(newTopology)
			c.logger.Info("Topology changed: %d brokers", newTopology.Length)
			if c.producerClient != nil {
				c.producerClient.OnNewTopology(newTopology)
			}
		}
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
		Length:             1,
		BrokerNames:        []string{name},
		ProducerPort:       t.ProducerPort,
		ConsumerPort:       t.ConsumerPort,
		ProducerBinaryPort: t.ProducerBinaryPort,
	}
}

func (c *Client) ProduceJson(topic string, message io.Reader, partitionKey string) error {
	t := c.Topology()
	ordinal := 0
	if partitionKey == "" {
		ordinal = c.getNextOrdinal(&c.producerIndex, t)
	} else {
		ordinal = c.getNaturalOwner(partitionKey, t)
	}

	bufferedMessage := utils.ToReadSeeker(message)
	initialPosition, err := bufferedMessage.Seek(0, io.SeekCurrent)
	if err != nil {
		// Seeking current position should be a safe operation, in any case, error out
		return err
	}

	maxAttempts := int(math.Min(float64(t.Length), 4))
	var lastErr error
	for i := 0; i < maxAttempts; i++ {
		if i > 0 {
			// Rewind the reader
			_, err := bufferedMessage.Seek(initialPosition, io.SeekStart)
			if err != nil {
				return err
			}
		}
		brokerOrdinal := (ordinal + i) % t.Length

		if !c.producerClient.IsProducerUp(brokerOrdinal) {
			c.logger.Debug("B%d is down, moving to next host", brokerOrdinal)
			lastErr = fmt.Errorf("Broker B%d is down", brokerOrdinal)
			continue
		}

		resp := c.producerClient.Send(brokerOrdinal, topic, bufferedMessage, partitionKey)
		if resp.Op() == producer.ProduceResponseOp {
			// Success
			return nil
		}
		if resp.Op() == producer.ErrorOp {
			lastErr = resp.(*producer.ErrorResponse).ToError()
			continue
		}
		lastErr = fmt.Errorf("Response op is not valid: %d", resp.Op())
	}
	return fmt.Errorf("No broker available: %d attempted, last error: %s", maxAttempts, lastErr)
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
	iterationDelay := 500 * time.Millisecond
	brokersLength := t.Length

	for attempt := 0; time.Since(start) < maxPollInterval; attempt++ {
		if time.Since(time.UnixMilli(atomic.LoadInt64(&c.lastConsumerError))) < iterationDelay {
			// Add a delay to prevent cycling through errors and consuming CPU
			c.logger.Info("Delaying next attempt after a consumer error")
			time.Sleep(jitter(iterationDelay))
		}

		responseChan := make(chan ConsumerPollResult, brokersLength)
		for i := 0; i < brokersLength; i++ {
			ordinal := i
			go func() {
				responseChan <- c.pollBroker(ordinal, ctxt, t)
			}()
		}

		errors := make([]error, 0)
		topicRecords := make([]TopicRecords, 0)
		for i := 0; i < brokersLength; i++ {
			r := <-responseChan
			if r.Error != nil {
				errors = append(errors, r.Error)
				// TODO: Set the Broker ordinal for the log message
				c.logger.Warn("Error occurred when polling broker: %s", r.Error)
				continue
			}

			if len(r.TopicRecords) > 0 {
				topicRecords = append(topicRecords, r.TopicRecords...)
			}
		}

		if len(topicRecords) > 0 {
			return ConsumerPollResult{TopicRecords: topicRecords}
		}

		if len(errors) == brokersLength {
			atomic.StoreInt64(&c.lastConsumerError, time.Now().UnixMilli())

			return ConsumerPollResult{
				Error: fmt.Errorf("All brokers polled resulted in error, first error: %s", errors[0]),
			}
		}

		// Wait for next attempt
		factor := 1
		if attempt > 2 {
			factor = 2
		}
		time.Sleep(jitter(iterationDelay * time.Duration(factor)))
	}

	return ConsumerPollResult{}
}

func (c *Client) pollBroker(ordinal int, ctxt context.Context, t *Topology) ConsumerPollResult {
	url := fmt.Sprintf("http://%s:%d%s", t.hostName(ordinal), t.ConsumerPort, consumerPollUrl)
	r, err := http.NewRequestWithContext(ctxt, http.MethodPost, url, nil)
	utils.PanicIfErr(err)
	resp, err := c.consumerClient.Do(r)
	if err != nil {
		if err == context.DeadlineExceeded || err == context.Canceled {
			return ConsumerPollResult{}
		}
		return ConsumerPollResult{Error: err}
	}

	if resp.StatusCode == http.StatusOK {
		topicRecords, err := serialization.ReadOkResponse(resp)
		if err != nil {
			// There was a serialization issue
			return ConsumerPollResult{Error: err}
		}
		if len(topicRecords) == 0 {
			// Kind of an edge case or at least it only happens with mocked data
			return ConsumerPollResult{}
		}
		return ConsumerPollResult{TopicRecords: topicRecords}
	}

	if resp.StatusCode == http.StatusNoContent {
		// Read and close body
		_, _ = utils.ReadBody(resp)

		// TODO: Get Retry-After
		return ConsumerPollResult{}
	}
	return ConsumerPollResult{Error: serialization.ReadErrorResponse(resp)}
}

// Performs a manual commit operation on all brokers in the cluster
func (c *Client) ManualCommit() ConsumerCommitResult {
	t := c.Topology()
	start := time.Now()
	maxPollInterval := c.consumerOptions.MaxPollInterval
	ctxt, _ := context.WithDeadline(context.Background(), start.Add(maxPollInterval))
	brokersLength := t.Length
	resultChan := make(chan BrokerError)

	for i := 0; i < brokersLength; i++ {
		ordinal := i
		go func() {
			resultChan <- c.manualCommitOnBroker(ordinal, ctxt, t)
		}()
	}

	result := ConsumerCommitResult{
		Errors: make([]BrokerError, 0),
	}

	for i := 0; i < brokersLength; i++ {
		err := <-resultChan
		if err != nil {
			result.Errors = append(result.Errors, err)
		} else {
			result.SuccessCount++
		}
	}

	return result
}

func (c *Client) manualCommitOnBroker(ordinal int, ctxt context.Context, t *Topology) BrokerError {
	url := fmt.Sprintf("http://%s:%d%s", t.hostName(ordinal), t.ConsumerPort, consumerManualCommitUrl)
	r, err := http.NewRequestWithContext(ctxt, http.MethodPost, url, nil)
	utils.PanicIfErr(err)
	resp, err := c.consumerClient.Do(r)
	if err != nil {
		return newBrokerError(err, ordinal)
	}

	if resp.StatusCode != http.StatusNoContent {
		return newBrokerError(serialization.ReadErrorResponse(resp), ordinal)
	}
	return nil
}

func (c *Client) Close() {
	c.logger.Info("PolarStreams client closing")
	atomic.StoreInt64(&c.isClosing, 1)
	c.discoveryClient.CloseIdleConnections()
	c.consumerClient.CloseIdleConnections()
	if c.producerClient != nil {
		c.producerClient.Close()
	}
}

func bodyClose(r *http.Response) {
	if r != nil && r.Body != nil {
		r.Body.Close()
	}
}

func (c *Client) getNextOrdinal(index *uint32, t *Topology) int {
	value := atomic.AddUint32(index, 1)
	if value >= maxAtomicIncrement {
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

func setDefaultOptions(options *ClientOptions) {
	if options.Logger == nil {
		options.Logger = NoopLogger
	}
	if options.TopologyPollInterval == 0 {
		options.TopologyPollInterval = DefaultTopologyPollInterval
	}
	if options.ProducerConnectionsPerHost == 0 {
		options.ProducerConnectionsPerHost = 1
	}
	if options.ProducerFlushThresholdBytes == 0 {
		options.ProducerFlushThresholdBytes = 64 * 1024
	}
}
