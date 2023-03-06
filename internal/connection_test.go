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
				requests:  make(chan *ProduceRequestPart, 10),
				logger:    StdLogger,
			}

			c.streamIds <- 0

			close(c.requests)

			resp := c.Send("topic1", bytes.NewReader([]byte("abc")), "key1")
			Expect(resp).To(Equal(NewClientErrorResponse("Request could not be sent: connection closed")))
		})

		It("should return when channel has data", func() {
			c := &connection{
				streamIds: make(chan StreamId, 10),
				requests:  make(chan *ProduceRequestPart, 10),
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

			part := <-c.requests
			part.Response <- NewEmptyResponse(ProduceResponseOp)

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
				requests:          make(chan *ProduceRequestPart, 10),
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

	Describe("appendToGroup", func() {
		It("should append to last group", func() {
			part1 := NewProduceRequestPart("t1", nil, "k1")
			part2 := NewProduceRequestPart("t1", nil, "k1")
			part3 := NewProduceRequestPart("t1", nil, "k1")
			part4 := NewProduceRequestPart("t2", nil, "k1")
			part5 := NewProduceRequestPart("t1", nil, "k2")
			part6 := NewProduceRequestPart("t1", nil, "")
			part7 := NewProduceRequestPart("t1", nil, "")

			group := appendToGroup([][]*ProduceRequestPart{}, part1)
			group = appendToGroup(group, part2)
			group = appendToGroup(group, part3)
			Expect(group).To(HaveLen(1))
			Expect(group[0]).To(HaveLen(3))
			group = appendToGroup(group, part4)
			Expect(group).To(HaveLen(2))
			Expect(group[1]).To(HaveLen(1))
			group = appendToGroup(group, part5)
			Expect(group).To(HaveLen(3))
			group = appendToGroup(group, part6)
			group = appendToGroup(group, part7)
			Expect(group).To(HaveLen(4))
			Expect(group[3]).To(HaveLen(2))
		})
	})
})

type fakeDisconnectHandler struct{}

func (f *fakeDisconnectHandler) OnConnectionClose(c *connection) {}
