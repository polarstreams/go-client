package internal

import (
	"math"
	"time"
)

const (
	baseReconnectionDelayMs = 20
	maxReconnectionDelayMs  = 30_000
)

type BackoffPolicy interface {
	Reset()

	// Returns the next delay
	Next() time.Duration
}

// Represents a default reconnection strategy
type exponentialBackoff struct {
	index int
}

func newExponentialBackoff() *exponentialBackoff {
	return &exponentialBackoff{}
}

func (p *exponentialBackoff) Reset() {
	p.index = 0
}

// Returns an exponential delay
func (p *exponentialBackoff) Next() time.Duration {
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

type fixedBackoff struct {
	delay time.Duration
}

func (p *fixedBackoff) Reset() {
}

// Returns an exponential delay
func (p *fixedBackoff) Next() time.Duration {
	return p.delay
}
