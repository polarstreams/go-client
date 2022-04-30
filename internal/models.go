package internal

import "fmt"

const contentType = "application/json"

type Topology struct {
	BaseName string `json:"baseName,omitempty"` // When defined, it represents the base name to build the broker names, e.g. "barco-"
	Length  int `json:"length"` // The ring size
	BrokerNames []string `json:"names,omitempty"`
	ProducerPort int `json:"producerPort"`
	ConsumerPort int `json:"consumerPort"`
}

func (t *Topology) ProducerUrl(topic string, ordinal int, partitionKey string) string {
	querystring := ""
	if partitionKey != "" {
		querystring = fmt.Sprintf("?partitionKey=%s", partitionKey)
	}
	return fmt.Sprintf("http://%s:%d/v1/topic/%s/messages%s", t.hostName(ordinal), t.ProducerPort, topic, querystring)
}

func (t *Topology) hostName(ordinal int) string {
	if len(t.BrokerNames) > 0 {
		return t.BrokerNames[ordinal]
	}

	return fmt.Sprintf("%s%d", t.BaseName, ordinal)
}
