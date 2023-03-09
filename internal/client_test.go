package internal

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/polarstreams/go-client/internal/serialization/producer"
	"github.com/polarstreams/go-client/types"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

const (
	partitionKeyT0Range = "123"
	partitionKeyT1Range = "567"
	partitionKeyT2Range = "234"
)
const topicName = "abc"

const reconnectionDelay = 20 * time.Millisecond
const additionalTestDelay = 500 * time.Millisecond

func Test(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Internal Suite")
}

var _ = Describe("Client", func() {
	// Note that on macos you need to manually create the alias for the loopback addresses, for example
	// for i in {2..3}; do sudo ifconfig lo0 alias 127.0.0.$i up; done
	Describe("NewClient()", func() {
		It("should return an error when service url is invalid", func() {
			_, err := NewClient("zzz://my-host:1234/", nil)
			Expect(err).To(HaveOccurred())
			_, err = NewClient("abc", nil)
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("Connect()", func() {
		var server *httptest.Server
		discoveryAddress := ""
		const baseName = "polar_sample-"
		var counter int64

		BeforeEach(func() {
			atomic.StoreInt64(&counter, 0)
			server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				topology := Topology{
					BaseName:           baseName,
					Length:             10 + int(atomic.AddInt64(&counter, 1)),
					ProducerPort:       8091,
					ConsumerPort:       8092,
					ProducerBinaryPort: 8093,
				}
				Expect(json.NewEncoder(w).Encode(topology)).NotTo(HaveOccurred())
			}))

			// Remove http://
			discoveryAddress = server.URL[7:]
		})

		AfterEach(func() {
			if server != nil {
				server.Close()
			}
		})

		It("should retrieve and store the topology", func() {
			client, err := NewClient(fmt.Sprintf("polar://%s", discoveryAddress), nil)
			Expect(err).NotTo(HaveOccurred())
			defer client.Close()
			Expect(client.Topology()).To(Equal(&Topology{
				BaseName:           baseName,
				Length:             11,
				ProducerPort:       8091,
				ConsumerPort:       8092,
				ProducerBinaryPort: 8093,
			}))
		})

		It("should start polling in the background", func() {
			const pollInterval = 100 * time.Millisecond
			client, err := NewClient(fmt.Sprintf("polar://%s", discoveryAddress), nil)
			Expect(err).NotTo(HaveOccurred())
			client.topologyPollInterval = pollInterval
			defer client.Close()
			time.Sleep(pollInterval * 4)
			Expect(client.Topology().BaseName).To(Equal(baseName))
			Expect(client.Topology().Length).To(BeNumerically(">", 11))
		})
	})

	Context("With a healthy cluster", func() {
		Describe("ProduceJson()", func() {
			var discoveryServer *httptest.Server
			var shutdown0, shutdown1, shutdown2 func()
			var c0, c1, c2 chan produceRequest
			topology := newTestTopology()
			discoveryAddress := ""

			BeforeEach(func() {
				discoveryServer = NewDiscoveryServer(topology)
				discoveryAddress = discoveryServer.URL[7:] // Remove http://
				shutdown0, c0 = NewProducerServerWithChannel("127.0.0.1:8093")
				shutdown1, c1 = NewProducerServerWithChannel("127.0.0.2:8093")
				shutdown2, c2 = NewProducerServerWithChannel("127.0.0.3:8093")
			})

			AfterEach(func() {
				discoveryServer.Close()
				shutdown0()
				shutdown1()
				shutdown2()
			})

			It("should send a request to each host in round robin", func() {
				client := newTestClient(discoveryAddress, true)
				defer client.Close()

				produceJson(client, `{"key0": "value0"}`, "")
				produceJson(client, `{"key1": "value1"}`, "")
				produceJson(client, `{"key2": "value2"}`, "")

				Expect(drainChan(c0)).To(Equal([]produceRequest{{topic: topicName, message: `{"key0": "value0"}`}}))
				Expect(drainChan(c1)).To(Equal([]produceRequest{{topic: topicName, message: `{"key1": "value1"}`}}))
				Expect(drainChan(c2)).To(Equal([]produceRequest{{topic: topicName, message: `{"key2": "value2"}`}}))
			})

			It("should send a request to each host according to the partition key", func() {
				client := newTestClient(discoveryAddress, true)
				defer client.Close()

				produceJson(client, `{"key0": "value0_0"}`, partitionKeyT0Range)
				produceJson(client, `{"key0": "value0_1"}`, partitionKeyT0Range)
				produceJson(client, `{"key0": "value0_2"}`, partitionKeyT0Range)
				produceJson(client, `{"key2": "value2"}`, partitionKeyT2Range)
				produceJson(client, `{"key1": "value1"}`, partitionKeyT1Range)

				Expect(drainChan(c0)).To(Equal([]produceRequest{{
					topic:        topicName,
					message:      `{"key0": "value0_0"}`,
					partitionKey: partitionKeyT0Range,
				}, {
					topic:        topicName,
					message:      `{"key0": "value0_1"}`,
					partitionKey: partitionKeyT0Range,
				}, {
					topic:        topicName,
					message:      `{"key0": "value0_2"}`,
					partitionKey: partitionKeyT0Range,
				}}))
				Expect(drainChan(c1)).To(Equal([]produceRequest{{
					topic:        topicName,
					message:      `{"key1": "value1"}`,
					partitionKey: partitionKeyT1Range,
				}}))
				Expect(drainChan(c2)).To(Equal([]produceRequest{{
					topic:        topicName,
					message:      `{"key2": "value2"}`,
					partitionKey: partitionKeyT2Range,
				}}))
			})
		})
	})

	Context("With hosts going up and down", func() {
		Describe("ProduceJson()", func() {

			Context("With a partial online cluster", func() {
				var discoveryServer *httptest.Server
				var shutdown0, shutdown1, shutdown2 func()
				var c0, c1, c2 chan produceRequest
				topology := Topology{
					Length:             3,
					BrokerNames:        []string{"127.0.0.1", "127.0.0.2", "127.0.0.3"},
					ProducerPort:       8091,
					ConsumerPort:       8092,
					ProducerBinaryPort: 8093,
				}
				discoveryAddress := ""

				BeforeEach(func() {
					discoveryServer = NewDiscoveryServer(topology)
					discoveryAddress = discoveryServer.URL[7:] // Remove http://
					shutdown1, c1 = NewProducerServerWithChannel("127.0.0.2:8093")
					shutdown2, c2 = NewProducerServerWithChannel("127.0.0.3:8093")
				})

				AfterEach(func() {
					discoveryServer.Close()
					shutdown1()
					shutdown2()
					if shutdown0 != nil {
						shutdown0()
					}
				})

				It("should route request according to the partition key or use the next host", func() {
					client := newTestClient(discoveryAddress, true)
					defer client.Close()

					// Host 0 is offline
					produceJson(client, `{"key0": "value0_0"}`, partitionKeyT0Range)
					produceJson(client, `{"key1": "value1"}`, partitionKeyT1Range)

					Expect(drainChan(c0)).To(Equal([]produceRequest{}))
					// The first message was rerouted to B1
					Expect(drainChan(c1)).To(Equal([]produceRequest{{
						topic:        topicName,
						message:      `{"key0": "value0_0"}`,
						partitionKey: partitionKeyT0Range,
					}, {
						topic:        topicName,
						message:      `{"key1": "value1"}`,
						partitionKey: partitionKeyT1Range,
					}}))
					Expect(drainChan(c2)).To(Equal([]produceRequest{}))
					Expect(client.isProducerUp(0)).To(BeFalse())
					Expect(client.isProducerUp(1)).To(BeTrue())
					Expect(client.isProducerUp(2)).To(BeTrue())

					time.Sleep(reconnectionDelay * 2)
					shutdown0, c0 = NewProducerServerWithChannel("127.0.0.1:8093")
					time.Sleep(reconnectionDelay + additionalTestDelay)
					Expect(client.isProducerUp(0)).To(BeTrue())

					produceJson(client, `{"key0": "value0_1"}`, partitionKeyT0Range)
					Expect(drainChan(c0)).To(Equal([]produceRequest{{
						topic:        topicName,
						message:      `{"key0": "value0_1"}`,
						partitionKey: partitionKeyT0Range,
					}}))
				})

				It("should reconnect after successful initial connection", func() {
					shutdown0, c0 = NewProducerServerWithChannel("127.0.0.1:8093")
					client := newTestClient(discoveryAddress, true)
					defer client.Close()

					// Host 0 is online
					produceJson(client, `{"key0": "value0_0"}`, partitionKeyT0Range)
					produceJson(client, `{"key1": "value1"}`, partitionKeyT1Range)

					Expect(drainChan(c0)).To(Equal([]produceRequest{{
						topic:        topicName,
						message:      `{"key0": "value0_0"}`,
						partitionKey: partitionKeyT0Range,
					}}))
					Expect(drainChan(c1)).To(Equal([]produceRequest{{
						topic:        topicName,
						message:      `{"key1": "value1"}`,
						partitionKey: partitionKeyT1Range,
					}}))
					Expect(drainChan(c2)).To(Equal([]produceRequest{}))

					// Shutdown B0
					shutdown0()
					time.Sleep(additionalTestDelay)

					produceJson(client, `{"key0": "value0_1"}`, partitionKeyT0Range)
					Expect(drainChan(c1)).To(Equal([]produceRequest{{
						topic:        topicName,
						message:      `{"key0": "value0_1"}`,
						partitionKey: partitionKeyT0Range,
					}}))

					Expect(client.isProducerUp(0)).To(BeFalse())
					Expect(client.isProducerUp(1)).To(BeTrue())
					Expect(client.isProducerUp(2)).To(BeTrue())

					shutdown0, c0 = NewProducerServerWithChannel("127.0.0.1:8093")
					time.Sleep(reconnectionDelay + additionalTestDelay)
					Expect(client.isProducerUp(0)).To(BeTrue())

					produceJson(client, `{"key0": "value0_2"}`, partitionKeyT0Range)
					Expect(drainChan(c0)).To(Equal([]produceRequest{{
						topic:        topicName,
						message:      `{"key0": "value0_2"}`,
						partitionKey: partitionKeyT0Range,
					}}))
				})
			})
		})
	})
})

func NewTestServer(address string, handler http.Handler) *http.Server {
	h2s := &http2.Server{}
	server := &http.Server{
		Addr:    address,
		Handler: h2c.NewHandler(handler, h2s),
	}

	err := http2.ConfigureServer(server, h2s)
	Expect(err).NotTo(HaveOccurred())

	go func() {
		server.ListenAndServe()
	}()
	return server
}

func NewProducerServerWithChannel(address string) (func(), chan produceRequest) {
	requests := make(chan produceRequest, 100)
	l, err := net.Listen("tcp", address)
	Expect(err).NotTo(HaveOccurred())
	go func() {
		connections := make([]net.Conn, 0)
		for {
			conn, err := l.Accept()
			if err != nil {
				log.Printf("Server at %s stopped accepting connections", address)
				break
			}

			connections = append(connections, conn)
			handleProducerConnection(conn, requests)
		}

		for _, c := range connections {
			_ = c.Close()
		}
	}()

	closeHandler := func() {
		_ = l.Close()
	}

	return closeHandler, requests
}

func handleProducerConnection(conn net.Conn, requests chan produceRequest) {
	responses := make(chan []byte, 100)
	go receiveRequests(conn, requests, responses)
	go sendResponses(conn, responses)
}

func receiveRequests(conn net.Conn, requests chan produceRequest, responses chan []byte) {
	// First request must be a Startup message
	initialized := false
	header := &producer.BinaryHeader{}

	for {
		err := binary.Read(conn, producer.Endianness, header)
		if err != nil {
			if err != io.EOF {
				log.Printf("Reading header from client failed: %s", err)
			}
			break
		}

		if !initialized {
			if header.Op == producer.StartupOp && header.StreamId == 0 && header.BodyLength == 0 {
				initialized = true
				responses <- createResponse(header, producer.ReadyOp)
				continue
			}

			log.Printf("Invalid first header: %#v", header)
			break
		}

		if header.Op == producer.ProduceOp {
			r, err := unmarshalRequest(header, conn)
			if err != nil {
				log.Printf("Reading body from client failed: %s", err)
				break
			}
			requests <- *r
			responses <- createResponse(header, producer.ProduceResponseOp)
			continue
		}

		panic("Received invalid operation in test server")
	}

	_ = conn.Close()
}

func sendResponses(conn net.Conn, responses chan []byte) {
	for res := range responses {
		_, err := conn.Write(res)
		if err != nil {
			log.Printf("Write to client failed: %s", err)
			break
		}
	}

	_ = conn.Close()
}

func createResponse(req *producer.BinaryHeader, op producer.OpCode) []byte {
	buf := new(bytes.Buffer)
	producer.WriteHeader(buf, &producer.BinaryHeader{
		Version:    1,
		Flags:      0,
		StreamId:   req.StreamId,
		Op:         op,
		BodyLength: 0,
		Crc:        0,
	})
	return buf.Bytes()
}

func unmarshalRequest(header *producer.BinaryHeader, conn net.Conn) (*produceRequest, error) {
	bodyBuf := make([]byte, header.BodyLength)
	_, err := io.ReadFull(conn, bodyBuf)
	if err != nil {
		return nil, err
	}
	partitionKey, index := readString(0, bodyBuf)
	topic, index := readString(index, bodyBuf)
	messageLength := int(producer.Endianness.Uint32(bodyBuf[index:]))
	index += 4

	return &produceRequest{
		topic:        topic,
		message:      string(bodyBuf[index : index+messageLength]),
		partitionKey: partitionKey,
	}, nil
}

func readString(index int, buf []byte) (string, int) {
	length := int(buf[index])
	end := index + 1 + length
	return string(buf[index+1 : end]), end
}

type produceRequest struct {
	topic        string
	message      string
	partitionKey string
}

func NewDiscoveryServer(topology Topology) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		Expect(json.NewEncoder(w).Encode(topology)).NotTo(HaveOccurred())
	}))
}

func produceJson(client *Client, message string, partitionKey string) {
	err := client.ProduceJson(topicName, strings.NewReader(message), partitionKey)
	Expect(err).NotTo(HaveOccurred())
}

func respBody(resp *http.Response) string {
	defer bodyClose(resp)
	body, err := io.ReadAll(resp.Body)
	Expect(err).NotTo(HaveOccurred())
	return string(body)
}

func reqBody(req *http.Request) string {
	body, err := io.ReadAll(req.Body)
	Expect(err).NotTo(HaveOccurred())
	return string(body)
}

func drainChan(c chan produceRequest) []produceRequest {
	result := make([]produceRequest, 0)
	hasData := true
	for hasData {
		select {
		case m := <-c:
			result = append(result, m)
		default:
			hasData = false
		}
	}
	return result
}

// Returns a connected client
func newTestClient(discoveryAddress string, withProducer bool) *Client {
	options := ClientOptions{
		Logger:                 types.StdLogger,
		FixedReconnectionDelay: reconnectionDelay,
		ProducerInitialize:     withProducer,
	}
	client, err := NewClient(fmt.Sprintf("polar://%s", discoveryAddress), &options)
	Expect(err).NotTo(HaveOccurred())
	return client
}

func newTestTopology() Topology {
	return Topology{
		Length:             3,
		BrokerNames:        []string{"127.0.0.1", "127.0.0.2", "127.0.0.3"},
		ProducerPort:       8091,
		ConsumerPort:       8092,
		ProducerBinaryPort: 8093,
	}
}
