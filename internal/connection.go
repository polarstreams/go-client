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

	. "github.com/polarstreams/go-client/internal/serialization/producer"
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

	w := bytes.NewBuffer(make([]byte, HeaderSize))
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
		requests:          make(chan BinaryRequest, 512),
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

func (c *connection) close() {
	c.closeOnce.Do(func() {
		_ = c.conn.Close()
		c.disconnectHandler.OnConnectionClose(c)
	})
}

func (c *connection) receiveResponses() {
	header := &BinaryHeader{}
	headerBuffer := make([]byte, HeaderSize)
	headerReader := bytes.NewReader(headerBuffer)
	bodyBuffer := make([]byte, ResponseBodyMaxLength)

	for {
		_, err := io.ReadFull(c.conn, headerBuffer)
		if err != nil {
			c.logger.Debug("Error reading from binary server: %s", err)
			break
		}
		headerReader.Reset(headerBuffer)
		err = binary.Read(headerReader, Endianness, &header)
		if err != nil {
			c.logger.Debug("Error reading header from binary server: %s", err)
			break
		}

		h, loaded := c.handlers.LoadAndDelete(header.StreamId)
		if !loaded {
			panic(fmt.Sprintf("No handler for stream id %d", header.StreamId))
		}
		handler := h.(streamHandler)
		var response BinaryResponse
		if header.BodyLength == 0 {
			response = NewEmptyResponse(header.Op)
		} else if header.Op == ErrorOp {
			buf := bodyBuffer[:header.BodyLength]
			_, err := io.ReadFull(c.conn, buf)
			if err != nil {
				c.logger.Debug("Error reading body from binary server: %s", err)
				break
			}
			response = &ErrorResponse{
				Code:    ErrorCode(buf[0]),
				Message: string(buf[1:]),
			}
		}

		handler(response)
	}
	c.close()
}

func totalRequestSize(r BinaryRequest) int {
	return r.BodyLength() + HeaderSize
}

func (c *connection) sendRequests() {
	w := bytes.NewBuffer(make([]byte, c.flushThreshold))

	shouldExit := false
	var item BinaryRequest
	for !shouldExit {
		w.Reset()
		groupSize := 0
		group := make([]BinaryRequest, 0)
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
				responseSize := totalRequestSize(request)
				if responseSize+w.Len() > c.flushThreshold {
					canAddNext = false
					item = request
					break
				}
				group = append(group, request)
				groupSize += responseSize

			default:
				canAddNext = false
			}
		}

		for _, request := range group {
			if err := request.Marshal(w); err != nil {
				c.logger.Warn("There was an error while marshaling a request, closing connection: %s", err)
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
	c.close()
}

type connectionSet map[*connection]bool

type disconnectHandler interface {
	OnConnectionClose(c *connection)
}

type streamHandler func(r BinaryResponse)

// Represents a group of connections to a single producer
type producerConnectionPool struct {
	address                  string
	length                   int          // The amount of expected connections
	connections              atomic.Value // Copy-on-write connections
	mu                       sync.Mutex
	isConnecting             int32
	isClosed                 int32
	logger                   Logger
	connectionFlushThreshold int
}

func newProducerConnectionPool(length int, connectionFlushThreshold int, logger Logger) *producerConnectionPool {
	p := &producerConnectionPool{
		length:                   length,
		connections:              atomic.Value{},
		mu:                       sync.Mutex{},
		isConnecting:             0,
		isClosed:                 0,
		logger:                   logger,
		connectionFlushThreshold: connectionFlushThreshold,
	}

	go p.startConnecting()
	return p
}

func (p *producerConnectionPool) Close() {
	atomic.StoreInt32(&p.isClosed, 1)
	p.mu.Lock()
	defer p.mu.Unlock()

	pool := p.getConnections()
	for c := range pool {
		c.close()
	}
}

func (p *producerConnectionPool) OnConnectionClose(c *connection) {
	// Use a different go routine as we might be holding the lock
	go func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.isPoolClosed() {
			p.addOrDelete(c, true)
		}
	}()
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

	backoff := newExponentialBackoff()
	existing := p.getConnections()
	for len(existing) < p.length && p.isPoolClosed() {
		c, err := newConnection(p.address, p, p.connectionFlushThreshold, p.logger)
		if err != nil {
			if p.isPoolClosed() {
				return
			}

			time.Sleep(backoff.next())
			continue
		}

		backoff.reset()
		p.mu.Lock()

		if p.isPoolClosed() {
			p.mu.Unlock()
			c.close()
			return
		}

		existing = p.addOrDelete(c, false)
		p.mu.Unlock()
	}
}

func (p *producerConnectionPool) getConnections() connectionSet {
	value := p.connections.Load()
	if value == nil {
		return nil
	}
	return value.(connectionSet)
}

// Adds or deletes a connection, callers MUST hold the lock
func (p *producerConnectionPool) addOrDelete(c *connection, deleteConnection bool) connectionSet {
	existingMap := p.getConnections()

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

	p.connections.Store(newMap)
	return newMap
}
