package metrics

import (
	"sync"
	"time"
)

type Metrics struct {
	mu                      sync.Mutex
	totalTasks              int
	successfulTasks         int
	failedTasks             int
	taskProcessingDurations []time.Duration
}

func NewMetrics() *Metrics {
	return &Metrics{
		taskProcessingDurations: make([]time.Duration, 0),
	}
}

func (m *Metrics) RecordTaskSuccess(duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.totalTasks++
	m.successfulTasks++
	m.taskProcessingDurations = append(m.taskProcessingDurations, duration)
}

func (m *Metrics) RecordTaskFailure(duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.totalTasks++
	m.failedTasks++
	m.taskProcessingDurations = append(m.taskProcessingDurations, duration)
}

func (m *Metrics) GetMetrics() (int, int, int, []time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.totalTasks, m.successfulTasks, m.failedTasks, m.taskProcessingDurations
}
