package retry

import (
	"math"
	"time"
)

type Backoff struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
	Factor    float64
}

func NewExponentialBackoff() *Backoff {
	return &Backoff{
		BaseDelay: 100 * time.Millisecond,
		MaxDelay:  60 * time.Second,
		Factor:    2.0,
	}
}

func (b *Backoff) GetDelay(attempt int) time.Duration {
	delay := time.Duration(float64(b.BaseDelay) * math.Pow(b.Factor, float64(attempt)))
	if delay > b.MaxDelay {
		return b.MaxDelay
	}
	return delay
}

const MaxRetries = 3
