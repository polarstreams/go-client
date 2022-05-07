package internal

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/barcostreams/go-client/internal/serialization"
	. "github.com/barcostreams/go-client/types"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Client", func() {
	Describe("RegisterAsConsumer()", func() {
		Context("with a healthy cluster", func() {
			var discoveryServer *httptest.Server
			var s0, s1, s2 *http.Server
			var c0, c1, c2 chan *http.Request
			topology := newTestTopology()
			discoveryAddress := ""

			BeforeEach(func() {
				discoveryServer = NewDiscoveryServer(topology)
				discoveryAddress = discoveryServer.URL[7:] // Remove http://
				s0, c0 = NewConsumerServerWithChannel("127.0.0.1:8092")
				s1, c1 = NewConsumerServerWithChannel("127.0.0.2:8092")
				s2, c2 = NewConsumerServerWithChannel("127.0.0.3:8092")
			})

			AfterEach(func() {
				discoveryServer.Close()
				s0.Shutdown(context.Background())
				s1.Shutdown(context.Background())
				s2.Shutdown(context.Background())
			})

			It("Subscribes in each discovered server", func() {
				client := newTestClient(discoveryAddress)
				defer client.Close()
				options := ConsumerOptions{
					Group:  "a",
					Id:     "b",
					Topics: []string{"c"},
				}
				client.RegisterAsConsumer(options)
				expectRegisterChan(c0, options)
				expectRegisterChan(c1, options)
				expectRegisterChan(c2, options)
			})

			It("Re-registers itself when a host goes DOWN and UP", func() {
				s0.Shutdown(context.Background())
				s0 = nil

				client := newTestClient(discoveryAddress)
				defer client.Close()
				options := ConsumerOptions{
					Group:  "a",
					Id:     "b",
					Topics: []string{"c"},
				}
				client.RegisterAsConsumer(options)
				Expect(drainRChan(c0)).To(HaveLen(0))
				expectRegisterChan(c1, options)
				expectRegisterChan(c2, options)

				t := client.Topology()
				Expect(client.isConsumerUp(0, t)).To(BeFalse())
				Expect(client.isConsumerUp(1, t)).To(BeTrue())
				Expect(client.isConsumerUp(2, t)).To(BeTrue())

				s0, c0 = NewConsumerServerWithChannel("127.0.0.1:8092")
				time.Sleep(reconnectionDelay + additionalTestDelay)
				Expect(client.isConsumerUp(0, t)).To(BeTrue())

				// We need to actively poll
				client.SendStatusRequestToConsumer()
				time.Sleep(additionalTestDelay)
				expectRegisterChan(c0, options)

				// The rest shouldn't receive new register attempts
				Expect(drainRChan(c1)).To(HaveLen(0))
				Expect(drainRChan(c2)).To(HaveLen(0))
			})
		})
	})
})

func NewConsumerServerWithChannel(address string) (*http.Server, chan *http.Request) {
	c := make(chan *http.Request, 100)
	server := NewTestServer(address, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte("OK"))
		if r.URL.Path == "/status" {
			log.Printf("Broker with address %s received status request", address)
			return
		}
		log.Printf("Received consumer request to %s", r.URL.Path)
		c <- r
	}))

	return server, c
}

func drainRChan(c chan *http.Request) []*http.Request {
	result := make([]*http.Request, 0)
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

func expectRegisterChan(c chan *http.Request, options ConsumerOptions) {
	requests := drainRChan(c)
	Expect(requests).To(HaveLen(1))
	r := requests[0]
	Expect(r.URL.Path).To(Equal(consumerRegisterUrl))
	var registerInfo serialization.RegisterConsumerInfo
	err := json.NewDecoder(r.Body).Decode(&registerInfo)
	Expect(err).NotTo(HaveOccurred())
	Expect(registerInfo).To(Equal(serialization.RegisterConsumerInfo{
		Id:     options.Id,
		Group:  options.Group,
		Topics: options.Topics,
	}))
}
