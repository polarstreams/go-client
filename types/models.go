package types

import (
	"fmt"
	"time"
)

// TopicDataId contains information to locate a certain piece of data.
//
// Specifies a topic, for a token, for a defined gen id.
type TopicDataId struct {
	Name       string     `json:"topic"`
	Token      Token      `json:"token"`
	RangeIndex RangeIndex `json:"rangeIndex"`
	Version    GenVersion `json:"version"`
}

func (t *TopicDataId) String() string {
	return fmt.Sprintf("'%s' %d/%d v%d", t.Name, t.Token, t.RangeIndex, t.Version)
}

// Represents a partition token
type Token int64

func (t Token) String() string {
	return fmt.Sprintf("%d", t)
}

// Represents an index within a token range
type RangeIndex uint8

func (t RangeIndex) String() string {
	return fmt.Sprintf("%d", t)
}

// Generation version
type GenVersion uint32

func (v GenVersion) String() string {
	return fmt.Sprintf("%d", v)
}

// Represents a set of records of a topic.
type TopicRecords struct {
	Topic   *TopicDataId
	Records []Record
}

// Represents a data record from a topic.
type Record struct {
	Timestamp time.Time
	Offset    int64
	Body      []byte
}

// Represents the result of a poll operation.
type ConsumerPollResult struct {
	TopicRecords []TopicRecords
	Error        error
}

// Represents the result of a manual commit operations to all brokers in the cluster.
type ConsumerCommitResult struct {
	// The amount of errors with the origin
	Errors []BrokerError

	// The number of manual commit requests that succeeded
	SuccessCount int
}

// Represents the options for creating a Consumer.
type ConsumerOptions struct {
	Group           string   // The consumer group
	Id              string   // A unique identifier for the consumer. When not set a random identifier will be generated.
	Topics          []string // The topics to subscribe to
	MaxPollInterval time.Duration
	Logger          Logger // The logger for the consumer to output the log messages
}

// Represents the additional options to set when creating a Producer.
type ProducerOptions struct {
	Logger Logger
}

// An error that includes the origin of the error
type BrokerError interface {
	error

	// The ordinal of the broker for the error
	BrokerOrdinal() int
}
