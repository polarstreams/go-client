package internal

import (
	"bytes"
	"sync"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/polarstreams/go-client/internal/serialization/producer"
	fakes "github.com/polarstreams/go-client/internal/test/fakes"
	. "github.com/polarstreams/go-client/types"
)

var _ = Describe("connection", func() {
	Describe("Send()", func() {
		It("should recover from closed request channel", func() {
			c := &connection{
				streamIds: make(chan StreamId, 10),
				requests:  make(chan BinaryRequest, 10),
				logger:    StdLogger,
			}

			c.streamIds <- 0

			close(c.requests)

			resp := c.Send("topic1", bytes.NewReader([]byte("abc")), "key1")
			Expect(resp).To(Equal(NewClientErrorResponse("Request could not be sent: connection closed")))
		})

		It("should return when handler is invoked", func() {
			c := &connection{
				streamIds: make(chan StreamId, 10),
				requests:  make(chan BinaryRequest, 10),
				logger:    StdLogger,
			}

			c.streamIds <- 0

			r := make(chan BinaryResponse, 1)
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				wg.Done()
				r <- c.Send("topic1", bytes.NewReader([]byte("abc")), "key1")
			}()
			wg.Wait()
			handler := c.getHandler(0)
			handler(NewEmptyResponse(ProduceResponseOp))

			select {
			case resp := <-r:
				Expect(resp).To(Equal(NewEmptyResponse(ProduceResponseOp)))
			case <-time.After(1 * time.Second):
				panic("Test timeout")
			}
		})
	})

	Describe("Close()", func() {
		It("should invoke pending handlers", func() {
			c := &connection{
				streamIds:         make(chan StreamId, 10),
				requests:          make(chan BinaryRequest, 10),
				logger:            StdLogger,
				conn:              &fakes.Connection{},
				disconnectHandler: &fakeDisconnectHandler{},
			}

			const pending = 3
			r := make(chan BinaryResponse, pending)
			var wg sync.WaitGroup
			for i := 0; i < pending; i++ {
				c.streamIds <- StreamId(i)
				wg.Add(1)
				go func() {
					wg.Done()
					r <- c.Send("topic1", bytes.NewReader([]byte("abc")), "key1")
				}()
			}

			wg.Wait()

			go c.Close()

			for i := 0; i < pending; i++ {
				select {
				case resp := <-r:
					Expect(resp).To(Equal(NewClientErrorResponse("Request could not be sent: connection closed")))
				case <-time.After(1 * time.Second):
					panic("Test timeout")
				}
			}
		})
	})
})

type fakeDisconnectHandler struct{}

func (f *fakeDisconnectHandler) OnConnectionClose(c *connection) {}
