package internal

import "fmt"

const contentType = "application/json"

type Topology struct {
	BaseName           string   `json:"baseName,omitempty"` // When defined, it represents the base name to build the broker names, e.g. "polar-"
	Length             int      `json:"length"`             // The ring size
	BrokerNames        []string `json:"names,omitempty"`
	ProducerPort       int      `json:"producerPort"`
	ProducerBinaryPort int      `json:"producerBinaryPort"`
	ConsumerPort       int      `json:"consumerPort"`
}

func (t *Topology) hostName(ordinal int) string {
	if len(t.BrokerNames) > 0 {
		return t.BrokerNames[ordinal]
	}

	return fmt.Sprintf("%s%d", t.BaseName, ordinal)
}

type defaultBrokerError struct {
	err           error
	brokerOrdinal int
}

func (e *defaultBrokerError) Error() string {
	return e.err.Error()
}

func (e *defaultBrokerError) BrokerOrdinal() int {
	return e.brokerOrdinal
}

func newBrokerError(err error, ordinal int) *defaultBrokerError {
	return &defaultBrokerError{
		err:           err,
		brokerOrdinal: ordinal,
	}
}
