package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/barcostreams/go-client/types"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

const (
	partitionKeyT0Range = "123"
	partitionKeyT1Range = "567"
	partitionKeyT2Range = "234"
)

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
		It("should parse the url and set the http client", func() {
			client, err := NewClient("barco://my-host:1234/", nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(client.discoveryUrl).To(Equal("http://my-host:1234/v1/brokers"))
		})

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
		const baseName = "barco_sample-"
		var counter int64

		BeforeEach(func() {
			atomic.StoreInt64(&counter, 0)
			server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				topology := Topology{
					BaseName:     baseName,
					Length:       10 + int(atomic.AddInt64(&counter, 1)),
					ProducerPort: 8091,
					ConsumerPort: 8092,
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
			client, err := NewClient(fmt.Sprintf("barco://%s", discoveryAddress), nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(client.Connect()).NotTo(HaveOccurred())
			defer client.Close()
			Expect(client.Topology()).To(Equal(&Topology{
				BaseName:     baseName,
				Length:       11,
				ProducerPort: 8091,
				ConsumerPort: 8092,
			}))
		})

		It("should start polling in the background", func() {
			const pollInterval = 100 * time.Millisecond
			client, err := NewClient(fmt.Sprintf("barco://%s", discoveryAddress), nil)
			Expect(err).NotTo(HaveOccurred())
			client.topologyPollInterval = pollInterval
			Expect(client.Connect()).NotTo(HaveOccurred())
			defer client.Close()
			time.Sleep(pollInterval * 4)
			Expect(client.Topology().BaseName).To(Equal(baseName))
			Expect(client.Topology().Length).To(BeNumerically(">", 11))
		})
	})

	Context("With a healthy cluster", func() {
		Describe("ProduceJson()", func() {
			var discoveryServer *httptest.Server
			var s0, s1, s2 *http.Server
			var c0, c1, c2 chan string
			topology := newTestTopology()
			discoveryAddress := ""

			BeforeEach(func() {
				discoveryServer = NewDiscoveryServer(topology)
				discoveryAddress = discoveryServer.URL[7:] // Remove http://
				s0, c0 = NewProducerServerWithChannel("127.0.0.1:8091")
				s1, c1 = NewProducerServerWithChannel("127.0.0.2:8091")
				s2, c2 = NewProducerServerWithChannel("127.0.0.3:8091")
			})

			AfterEach(func() {
				discoveryServer.Close()
				s0.Shutdown(context.Background())
				s1.Shutdown(context.Background())
				s2.Shutdown(context.Background())
			})

			It("should send a request to each host in round robin", func() {
				client := newTestClient(discoveryAddress)
				defer client.Close()

				produceJson(client, `{"key0": "value0"}`, "")
				produceJson(client, `{"key1": "value1"}`, "")
				produceJson(client, `{"key2": "value2"}`, "")

				Expect(drainChan(c0)).To(Equal([]string{`{"key0": "value0"}`}))
				Expect(drainChan(c1)).To(Equal([]string{`{"key1": "value1"}`}))
				Expect(drainChan(c2)).To(Equal([]string{`{"key2": "value2"}`}))
			})

			It("should send a request to each host according to the partition key", func() {
				client := newTestClient(discoveryAddress)
				defer client.Close()

				produceJson(client, `{"key0": "value0_0"}`, partitionKeyT0Range)
				produceJson(client, `{"key0": "value0_1"}`, partitionKeyT0Range)
				produceJson(client, `{"key0": "value0_2"}`, partitionKeyT0Range)
				produceJson(client, `{"key2": "value2"}`, partitionKeyT2Range)
				produceJson(client, `{"key1": "value1"}`, partitionKeyT1Range)

				Expect(drainChan(c0)).To(Equal([]string{`{"key0": "value0_0"}`, `{"key0": "value0_1"}`, `{"key0": "value0_2"}`}))
				Expect(drainChan(c1)).To(Equal([]string{`{"key1": "value1"}`}))
				Expect(drainChan(c2)).To(Equal([]string{`{"key2": "value2"}`}))
			})
		})
	})

	Context("With hosts going up and down", func() {
		Describe("ProduceJson()", func() {

			Context("With a partial online cluster", func() {
				var discoveryServer *httptest.Server
				var s0, s1, s2 *http.Server
				var c0, c1, c2 chan string
				topology := Topology{
					Length:       3,
					BrokerNames:  []string{"127.0.0.1", "127.0.0.2", "127.0.0.3"},
					ProducerPort: 8091,
					ConsumerPort: 8092,
				}
				discoveryAddress := ""

				BeforeEach(func() {
					discoveryServer = NewDiscoveryServer(topology)
					discoveryAddress = discoveryServer.URL[7:] // Remove http://
					s1, c1 = NewProducerServerWithChannel("127.0.0.2:8091")
					s2, c2 = NewProducerServerWithChannel("127.0.0.3:8091")
				})

				AfterEach(func() {
					discoveryServer.Close()
					s1.Shutdown(context.Background())
					s2.Shutdown(context.Background())
					if s0 != nil {
						s0.Shutdown(context.Background())
					}
				})

				It("should route request according to the partition key or use the next host", func() {
					client := newTestClient(discoveryAddress)
					defer client.Close()

					// Host 0 is offline
					produceJson(client, `{"key0": "value0_0"}`, partitionKeyT0Range)
					produceJson(client, `{"key1": "value1"}`, partitionKeyT1Range)

					Expect(drainChan(c0)).To(Equal([]string{}))
					// The first message was rerouted to B1
					Expect(drainChan(c1)).To(Equal([]string{`{"key0": "value0_0"}`, `{"key1": "value1"}`}))
					Expect(drainChan(c2)).To(Equal([]string{}))
					t := client.Topology()
					Expect(client.isProducerUp(0, t)).To(BeFalse())
					Expect(client.isProducerUp(1, t)).To(BeTrue())
					Expect(client.isProducerUp(2, t)).To(BeTrue())

					time.Sleep(reconnectionDelay * 2)
					s0, c0 = NewProducerServerWithChannel("127.0.0.1:8091")
					time.Sleep(reconnectionDelay + additionalTestDelay)
					Expect(client.isProducerUp(0, t)).To(BeTrue())

					produceJson(client, `{"key0": "value0_1"}`, partitionKeyT0Range)
					Expect(drainChan(c0)).To(Equal([]string{`{"key0": "value0_1"}`}))
				})

				It("should reconnect after successful initial connection", func() {
					s0, c0 = NewProducerServerWithChannel("127.0.0.1:8091")
					client := newTestClient(discoveryAddress)
					defer client.Close()

					// Host 0 is offline
					produceJson(client, `{"key0": "value0_0"}`, partitionKeyT0Range)
					produceJson(client, `{"key1": "value1"}`, partitionKeyT1Range)

					Expect(drainChan(c0)).To(Equal([]string{`{"key0": "value0_0"}`}))
					Expect(drainChan(c1)).To(Equal([]string{`{"key1": "value1"}`}))
					Expect(drainChan(c2)).To(Equal([]string{}))

					s0.Shutdown(context.Background())
					produceJson(client, `{"key0": "value0_1"}`, partitionKeyT0Range)
					Expect(drainChan(c1)).To(Equal([]string{`{"key0": "value0_1"}`}))

					t := client.Topology()
					Expect(client.isProducerUp(0, t)).To(BeFalse())
					Expect(client.isProducerUp(1, t)).To(BeTrue())
					Expect(client.isProducerUp(2, t)).To(BeTrue())

					s0, c0 = NewProducerServerWithChannel("127.0.0.1:8091")
					time.Sleep(reconnectionDelay + additionalTestDelay)
					Expect(client.isProducerUp(0, t)).To(BeTrue())

					produceJson(client, `{"key0": "value0_1"}`, partitionKeyT0Range)
					Expect(drainChan(c0)).To(Equal([]string{`{"key0": "value0_1"}`}))
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

func NewProducerServerWithChannel(address string) (*http.Server, chan string) {
	c := make(chan string, 100)
	server := NewTestServer(address, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte("OK"))
		if r.URL.Path == "/status" {
			log.Printf("Broker with address %s received status request", address)
			return
		}
		c <- reqBody(r)
	}))

	return server, c
}

func NewDiscoveryServer(topology Topology) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		Expect(json.NewEncoder(w).Encode(topology)).NotTo(HaveOccurred())
	}))
}

func produceJson(client *Client, message string, partitionKey string) {
	resp, err := client.ProduceJson("abc", strings.NewReader(message), partitionKey)
	Expect(err).NotTo(HaveOccurred())
	Expect(resp.StatusCode).To(Equal(http.StatusOK))
	Expect(respBody(resp)).To(Equal("OK"))
}

func respBody(resp *http.Response) string {
	defer bodyClose(resp)
	body, err := ioutil.ReadAll(resp.Body)
	Expect(err).NotTo(HaveOccurred())
	return string(body)
}

func reqBody(req *http.Request) string {
	body, err := ioutil.ReadAll(req.Body)
	Expect(err).NotTo(HaveOccurred())
	return string(body)
}

func drainChan(c chan string) []string {
	result := make([]string, 0)
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
func newTestClient(discoveryAddress string) *Client {
	options := ClientOptions{
		Logger:                 types.StdLogger,
		FixedReconnectionDelay: reconnectionDelay,
	}
	client, err := NewClient(fmt.Sprintf("barco://%s", discoveryAddress), &options)
	Expect(err).NotTo(HaveOccurred())
	Expect(client.Connect()).NotTo(HaveOccurred())
	return client
}

func newTestTopology() Topology {
	return Topology{
		Length:       3,
		BrokerNames:  []string{"127.0.0.1", "127.0.0.2", "127.0.0.3"},
		ProducerPort: 8091,
		ConsumerPort: 8092,
	}
}
