package retry

import (
	"math/rand"
	"time"
)

type Backoff struct {
	BaseDelay time.Duration

	MaxDelay time.Duration

	Factor float64
}

func NewBackoff(baseDelay, maxDelay time.Duration, factor float64) *Backoff {
	return &Backoff{
		BaseDelay: baseDelay,
		MaxDelay:  maxDelay,
		Factor:    factor,
	}
}

func (b *Backoff) GetDelay(attempt int) time.Duration {
	delay := time.Duration(float64(b.BaseDelay) * pow(b.Factor, float64(attempt)))
	if delay > b.MaxDelay {
		return b.MaxDelay
	}
	return delay
}

func pow(x, y float64) float64 {
	return rand.Float64() * (x * y)
}
