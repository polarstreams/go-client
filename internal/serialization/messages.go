package serialization

import (
	"fmt"
	"net/http"

	"github.com/polarstreams/go-client/internal/utils"
)

// Message for registering a consumer
type RegisterConsumerInfo struct {
	Id     string   `json:"id"`
	Group  string   `json:"group"`
	Topics []string `json:"topics"`
}

func ReadErrorResponse(resp *http.Response) error {
	body, err := utils.ReadBody(resp)
	if err != nil {
		// Serialization error
		return err
	}
	return fmt.Errorf("Unexpected error %s: %s", resp.Status, body)
}
