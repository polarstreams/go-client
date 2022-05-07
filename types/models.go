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

// Represents an index in the token range
type RangeIndex uint8

func (t RangeIndex) String() string {
	return fmt.Sprintf("%d", t)
}

type GenVersion uint32

func (v GenVersion) String() string {
	return fmt.Sprintf("%d", v)
}

type TopicRecords struct {
	Topic   *TopicDataId
	Records []Record
}

type Record struct {
	Timestamp time.Time
	Body      []byte
}

type ConsumerPollResult struct {
	TopicRecords []TopicRecords
	Error        error
}

type ConsumerOptions struct {
	Group           string
	Id              string
	Topics          []string
	MaxPollInterval time.Duration
}
