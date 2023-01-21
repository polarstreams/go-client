package internal

import (
	"math"
	"time"
)

const (
	baseReconnectionDelayMs = 20
	maxReconnectionDelayMs  = 30_000
)

// Represents a default reconnection strategy
type exponentialBackoff struct {
	index int
}

func newExponentialBackoff() *exponentialBackoff {
	return &exponentialBackoff{}
}

func (p *exponentialBackoff) reset() {
	p.index = 0
}

// Returns an exponential delay
func (p *exponentialBackoff) next() time.Duration {
	p.index++

	delayMs := maxReconnectionDelayMs
	if p.index < 53 {
		delayMs = int(math.Pow(2, float64(p.index))) * baseReconnectionDelayMs
		if delayMs > maxReconnectionDelayMs {
			delayMs = maxReconnectionDelayMs
		}
	}

	return time.Duration(delayMs) * time.Millisecond
}
