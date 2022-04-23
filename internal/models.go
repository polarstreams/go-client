package internal

type Topology struct {
	BaseName string `json:"baseName,omitempty"` // When defined, it represents the base name to build the broker names, e.g. "barco-"
	Length  int `json:"length"` // The ring size
	BrokerNames []string `json:"names,omitempty"`
	ProducerPort int `json:"producerPort"`
	ConsumerPort int `json:"consumerPort"`
}
