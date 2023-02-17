package internal

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/polarstreams/go-client/internal/serialization/producer"
	. "github.com/polarstreams/go-client/internal/types"
	. "github.com/polarstreams/go-client/types"
)

// Represents a set of connections per host that can send requests to a producer server
type producerClient struct {
	topology           atomic.Value
	connectionsPerHost int

	// Copy on write semantics for connection pools
	pools atomic.Value
	mu    sync.Mutex
}

type poolMap map[int]*producerConnectionPool

func newProducerClient(
	t *Topology,
	connectionsPerHost int,
	flushThresholdBytes int,
	fixedReconnectionDelay time.Duration,
	logger Logger,
) *producerClient {
	topology := atomic.Value{}
	topology.Store(t)

	pools := atomic.Value{}
	m := make(poolMap, t.Length)
	wgInit := sync.WaitGroup{}
	for i := 0; i < t.Length; i++ {
		address := fmt.Sprintf("%s:%d", t.hostName(i), t.ProducerBinaryPort)
		wgInit.Add(1)
		m[i] = newProducerConnectionPool(
			address,
			connectionsPerHost,
			flushThresholdBytes,
			logger,
			fixedReconnectionDelay,
			func() {
				wgInit.Done()
			},
		)
	}
	wgInit.Wait()
	pools.Store(m)

	logger.Debug("Created producer client to target %d brokers and %d connections each", t.Length, connectionsPerHost)

	return &producerClient{
		topology:           topology,
		connectionsPerHost: connectionsPerHost,
		pools:              pools,
		mu:                 sync.Mutex{},
	}
}

// Return the pool or nil
func (c *producerClient) getPool(ordinal int) *producerConnectionPool {
	value := c.pools.Load()
	if value == nil {
		return nil
	}
	m := value.(poolMap)
	return m[ordinal]
}

// Gets the current snapshot of the topology
func (c *producerClient) Topology() *Topology {
	return c.topology.Load().(*Topology)
}

func (c *producerClient) IsProducerUp(ordinal int) bool {
	pool := c.getPool(ordinal)
	return pool != nil && pool.IsConnected()
}

func (c *producerClient) Close() {
	defer c.mu.Unlock()
	c.mu.Lock()

	pools := c.pools.Swap(make(poolMap))
	for _, pool := range pools.(poolMap) {
		pool.Close()
	}
}

func (c *producerClient) OnNewTopology(newTopology *Topology) {
	c.topology.Store(newTopology)
}

// Gets an available connection and sends the request
func (c *producerClient) Send(
	ordinal int,
	topic string,
	message FixedLengthReader,
	partitionKey string,
) BinaryResponse {
	pool := c.getPool(ordinal)
	if pool == nil {
		return NewClientErrorResponse("No connection available")
	}
	conn := pool.NextConnection()
	if conn == nil {
		return NewClientErrorResponse("No connection available")
	}

	return conn.Send(topic, message, partitionKey)
}
