package internal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/polarstreams/go-client/internal/serialization/producer"
	. "github.com/polarstreams/go-client/internal/serialization/producer"
	. "github.com/polarstreams/go-client/internal/types"
	. "github.com/polarstreams/go-client/types"
)

const maxStreamIds = 1024

// Represents TCP connection to a producer
type connection struct {
	conn              net.Conn
	disconnectHandler disconnectHandler
	closeOnce         sync.Once
	handlers          sync.Map
	streamIds         chan StreamId
	requests          chan BinaryRequest
	logger            Logger
	flushThreshold    int
}

func newConnection(address string, h disconnectHandler, flushThreshold int, logger Logger) (*connection, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}

	w := bytes.NewBuffer(make([]byte, 0, HeaderSize))
	if err := WriteHeader(w, &BinaryHeader{
		Version:  1,
		StreamId: 0,
		Op:       StartupOp,
	}); err != nil {
		_ = conn.Close()
		return nil, err
	}

	_, err = conn.Write(w.Bytes())
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	c := &connection{
		conn:              conn,
		disconnectHandler: h,
		closeOnce:         sync.Once{},
		handlers:          sync.Map{},
		logger:            logger,
		streamIds:         make(chan StreamId, maxStreamIds),
		flushThreshold:    flushThreshold,
		requests:          make(chan BinaryRequest, 128),
	}

	// Reserve StreamId(0) for the Startup message
	for i := StreamId(1); i < maxStreamIds; i++ {
		c.streamIds <- i
	}

	go c.receiveResponses()

	response := make(chan BinaryResponse, 1)
	c.handlers.Store(StreamId(0), func(r BinaryResponse) {
		response <- r
	})

	r := <-response
	if r.Op() != ReadyOp {
		_ = conn.Close()
		if r.Op() == ErrorOp {
			return nil, r.(*ErrorResponse).ToError()
		}
		return nil, fmt.Errorf("Invalid response from server, expected READY, obtained: %d", r.Op())
	}

	go c.sendRequests()

	return c, nil
}

// Creates the response handler, appends the request and waits for the response
func (c *connection) Send(topic string, message FixedLengthReader, partitionKey string) (resp BinaryResponse) {
	defer func() {
		if r := recover(); r != nil {
			resp = NewClientErrorResponse("Request could not be sent: connection closed")
		}
	}()

	streamId := <-c.streamIds
	req := producer.NewProduceRequest(streamId, topic, message, partitionKey)
	response := make(chan BinaryResponse, 1)
	c.handlers.Store(streamId, func(r BinaryResponse) {
		response <- r
	})

	// Append the request, it might panic when requests channel is closed
	c.requests <- req
	// Wait for the response
	resp = <-response
	return resp
}

func (c *connection) Close() {
	c.closeOnce.Do(func() {
		c.logger.Debug("Connection to %s closed", c.conn.RemoteAddr().String())
		close(c.requests)
		_ = c.conn.Close()
		c.disconnectHandler.OnConnectionClose(c)
	})

	toDelete := make([]StreamId, 0)
	c.handlers.Range(func(key, value interface{}) bool {
		toDelete = append(toDelete, key.(StreamId))
		return true
	})

	for _, streamId := range toDelete {
		h, loaded := c.handlers.LoadAndDelete(streamId)
		if !loaded {
			continue
		}
		handler := h.(func(BinaryResponse))
		handler(NewClientErrorResponse("Request could not be sent: connection closed"))
	}
}

func (c *connection) receiveResponses() {
	header := &BinaryHeader{}
	bodyBuffer := make([]byte, ResponseBodyMaxLength)

	for {
		if err := binary.Read(c.conn, Endianness, header); err != nil {
			if err != io.EOF {
				c.logger.Debug("Error reading header from binary server: %s", err)
			}
			break
		}

		handler := c.getHandler(header.StreamId)
		var response BinaryResponse
		if header.BodyLength == 0 {
			response = NewEmptyResponse(header.Op)
		} else if header.Op == ErrorOp {
			buf := bodyBuffer[:header.BodyLength]
			_, err := io.ReadFull(c.conn, buf)
			if err != nil {
				// The handler was dequeued, surface the error
				handler(NewClientErrorResponse("Error reading body from server"))
				c.logger.Debug("Error reading body from binary server: %s", err)
				break
			}
			response = &ErrorResponse{
				Code:    ErrorCode(buf[0]),
				Message: string(buf[1:]),
			}
		}

		handler(response)

		// StreamId can be reused
		c.streamIds <- header.StreamId
	}

	c.Close()
}

// Gets and deletes the handler from the pending handlers
func (c *connection) getHandler(id StreamId) streamHandler {
	h, loaded := c.handlers.LoadAndDelete(id)
	if !loaded {
		panic(fmt.Sprintf("No handler for stream id %d", id))
	}
	return h.(func(BinaryResponse))
}

func totalRequestSize(r BinaryRequest) int {
	return r.BodyLength() + HeaderSize
}

func (c *connection) sendRequests() {
	w := bytes.NewBuffer(make([]byte, c.flushThreshold))
	header := &BinaryHeader{Version: 1, Flags: 0} // Reuse allocation

	shouldExit := false
	var item BinaryRequest
	var group []BinaryRequest
	for !shouldExit {
		w.Reset()
		groupSize := 0
		group = make([]BinaryRequest, 0)
		canAddNext := true

		if item == nil {
			// Block for the first item
			var ok bool
			item, ok = <-c.requests
			if !ok {
				break
			}
		}

		group = append(group, item)
		groupSize += totalRequestSize(item)
		item = nil

		// Coalesce requests w/ Nagle disabled
		for canAddNext && !shouldExit {
			select {
			case request, ok := <-c.requests:
				if !ok {
					shouldExit = true
					break
				}
				requestSize := totalRequestSize(request)
				if groupSize+requestSize > c.flushThreshold {
					canAddNext = false
					item = request
					break
				}
				group = append(group, request)
				groupSize += requestSize

			default:
				canAddNext = false
			}
		}

		for _, request := range group {
			if err := request.Marshal(w, header); err != nil {
				c.logger.Error("Error marshaling a request, closing connection: %s", err)
				shouldExit = true
				break
			}
		}

		if w.Len() > 0 {
			if _, err := c.conn.Write(w.Bytes()); err != nil {
				c.logger.Warn("There was an error while writing to a producer server, closing connection: %s", err)
				break
			}
		}
	}

	// Close in-flight group
	for _, request := range group {
		streamId := request.StreamId()
		handler := c.getHandler(streamId)
		handler(NewClientErrorResponse("Error while sending request"))
	}

	c.Close()
}

type connectionSet map[*connection]bool

type disconnectHandler interface {
	OnConnectionClose(c *connection)
}

type streamHandler func(r BinaryResponse)

// Represents a group of connections to a single producer
type producerConnectionPool struct {
	address                  string
	length                   int           // The amount of expected connections
	connectionsSnapshot      atomic.Value  // Connections collection with snapshot semantics ([]*connection)
	connections              connectionSet // Copy-on-write connections, must be accessed taking the lock
	mu                       sync.Mutex
	isConnecting             int32
	isClosed                 int32
	logger                   Logger
	connectionFlushThreshold int
	index                    uint32        // Used for round robin through connections
	initHandler              func()        // Invoked once after the first connection attempt is made (success or error)
	initOnce                 sync.Once     // Use to control the init handler call
	reconnectionBackoff      BackoffPolicy // Reconnection backoff
}

func newProducerConnectionPool(
	address string,
	length int,
	connectionFlushThreshold int,
	logger Logger,
	fixedReconnectionDelay time.Duration, // To simplify test
	initHandler func(),
) *producerConnectionPool {
	var reconnectionBackoff BackoffPolicy = newExponentialBackoff()
	if fixedReconnectionDelay > 0 {
		reconnectionBackoff = &fixedBackoff{delay: fixedReconnectionDelay}
	}

	p := &producerConnectionPool{
		address:                  address,
		length:                   length,
		connectionsSnapshot:      atomic.Value{},
		connections:              map[*connection]bool{},
		mu:                       sync.Mutex{},
		isConnecting:             0,
		isClosed:                 0,
		logger:                   logger,
		connectionFlushThreshold: connectionFlushThreshold,
		index:                    0,
		initHandler:              initHandler,
		initOnce:                 sync.Once{},
		reconnectionBackoff:      reconnectionBackoff,
	}

	go p.startConnecting()
	return p
}

func (p *producerConnectionPool) Close() {
	atomic.StoreInt32(&p.isClosed, 1)
	p.mu.Lock()
	defer p.mu.Unlock()

	pool := p.connections
	for c := range pool {
		c.Close()
	}
}

func (p *producerConnectionPool) OnConnectionClose(c *connection) {
	// Use a different go routine as we might be holding the lock
	go func() {
		if p.isPoolClosed() {
			return
		}

		// Remove the existing connection from the pool
		p.mu.Lock()
		p.addOrDelete(c, true)
		p.mu.Unlock()

		// Start reconnecting
		p.startConnecting()
	}()
}

// Gets the next available connection or nil
func (p *producerConnectionPool) NextConnection() *connection {
	connections := p.getConnections()
	length := len(connections)
	if length == 0 {
		return nil
	}
	if length == 1 {
		return connections[0]
	}

	index := atomic.AddUint32(&p.index, 1)
	if index >= maxAtomicIncrement {
		// Atomic inc operations don't wrap around, reset it (good-enough fairness)
		atomic.StoreUint32(&p.index, 0)
	}
	return connections[(int(index)-1)%length]

}

// Determines whether the pool is closed, we should hold the lock in case the check is important
func (p *producerConnectionPool) isPoolClosed() bool {
	return atomic.LoadInt32(&p.isClosed) == 1
}

func (p *producerConnectionPool) startConnecting() {
	if !atomic.CompareAndSwapInt32(&p.isConnecting, 0, 1) {
		// It's already connecting
		return
	}

	defer atomic.StoreInt32(&p.isConnecting, 0)

	p.reconnectionBackoff.Reset()
	existing := p.getConnections()

	for len(existing) < p.length && !p.isPoolClosed() {
		p.logger.Info("Creating new producer connection to %s", p.address)
		c, err := newConnection(p.address, p, p.connectionFlushThreshold, p.logger)

		// Mark as initialized, even on error
		p.initOnce.Do(p.initHandler)

		if err != nil {
			if p.isPoolClosed() {
				return
			}

			p.logger.Info("Error while opening a new connection to %s: %s", p.address, err.Error())
			time.Sleep(p.reconnectionBackoff.Next())
			continue
		}

		p.reconnectionBackoff.Reset()
		p.mu.Lock()

		if p.isPoolClosed() {
			p.mu.Unlock()
			c.Close()
			return
		}

		existing = p.addOrDelete(c, false)
		p.mu.Unlock()
		p.logger.Info("Created connection to %s (%d total)", p.address, len(existing))
	}
}

func (p *producerConnectionPool) getConnections() []*connection {
	value := p.connectionsSnapshot.Load()
	if value == nil {
		return nil
	}
	return value.([]*connection)
}

func (p *producerConnectionPool) IsConnected() bool {
	return len(p.getConnections()) > 0
}

// Adds or deletes a connection, callers MUST hold the lock
func (p *producerConnectionPool) addOrDelete(c *connection, deleteConnection bool) []*connection {
	existingMap := p.connections

	// Shallow copy existing
	newMap := make(connectionSet, len(existingMap)+1)
	for k, v := range existingMap {
		newMap[k] = v
	}

	if deleteConnection {
		delete(newMap, c)
	} else {
		newMap[c] = true
	}

	snapshot := make([]*connection, 0, len(newMap))
	for c := range newMap {
		snapshot = append(snapshot, c)
	}

	p.connections = newMap
	p.connectionsSnapshot.Store(snapshot)
	return snapshot
}
