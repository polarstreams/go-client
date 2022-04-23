package internal

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func Test(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Internal Suite")
}

var _ = Describe("Client", func() {
	Describe("NewClient()", func ()  {
		It("should parse the url and set the http client", func() {
			client, err := NewClient("barco://my-host:1234/")
			Expect(err).NotTo(HaveOccurred())
			Expect(client.discoveryUrl).To(Equal("http://my-host:1234/v1/brokers"))
		})

		It("should return an error when service url is invalid", func() {
			_, err := NewClient("zzz://my-host:1234/")
			Expect(err).To(HaveOccurred())
			_, err = NewClient("abc")
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("Connect()", func ()  {
		var server *httptest.Server
		serverHost := ""
		const baseName = "barco_sample-"
		var counter int64

		BeforeEach(func ()  {
			atomic.StoreInt64(&counter, 0)
			server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				topology := Topology{
					BaseName:    baseName,
					Length:      10 + int(atomic.AddInt64(&counter, 1)),
					ProducerPort: 8001,
					ConsumerPort: 8002,
				}
				Expect(json.NewEncoder(w).Encode(topology)).NotTo(HaveOccurred())
			}))

			// Remove http://
			serverHost = server.URL[7:]
		})

		AfterEach(func ()  {
			if server != nil {
				server.Close()
			}
		})

		It("Should retrieve and store the topology", func ()  {
			client, err := NewClient(fmt.Sprintf("barco://%s", serverHost))
			Expect(err).NotTo(HaveOccurred())
			Expect(client.Connect()).NotTo(HaveOccurred())
			defer client.Close()
			Expect(client.Topology()).To(Equal(Topology{
				BaseName: baseName,
				Length:   11,
				ProducerPort: 8001,
				ConsumerPort: 8002,
			}))
		})

		It("Should start polling in the background", func ()  {
			const pollInterval = 100 * time.Millisecond
			client, err := NewClient(fmt.Sprintf("barco://%s", serverHost))
			Expect(err).NotTo(HaveOccurred())
			client.pollInterval = pollInterval
			Expect(client.Connect()).NotTo(HaveOccurred())
			defer client.Close()
			time.Sleep(pollInterval * 4)
			Expect(client.Topology().BaseName).To(Equal(baseName))
			Expect(client.Topology().Length).To(BeNumerically(">", 11))
		})
	})
})
