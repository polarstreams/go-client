package serialization

// Message for registering a consumer
type RegisterConsumerInfo struct {
	Id     string   `json:"id"`
	Group  string   `json:"group"`
	Topics []string `json:"topics"`
}
