package metrics

import (
	"sort"
	"sync"
	"time"
)

type Metrics struct {
	mu                      sync.Mutex
	totalTasks              int
	successfulTasks         int
	failedTasks             int
	taskProcessingDurations []time.Duration
	maxStoredDurations      int
	totalProcessingTime     time.Duration
	minProcessingTime       time.Duration
	maxProcessingTime       time.Duration
}

func NewMetrics() *Metrics {
	return &Metrics{
		taskProcessingDurations: make([]time.Duration, 0),
		maxStoredDurations:      1000,
		minProcessingTime:       time.Duration(1<<63 - 1),
	}
}

func (m *Metrics) RecordTaskSuccess(duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.totalTasks++
	m.successfulTasks++
	m.recordDuration(duration)
}

func (m *Metrics) RecordTaskFailure(duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.totalTasks++
	m.failedTasks++
	m.recordDuration(duration)
}

func (m *Metrics) recordDuration(duration time.Duration) {
	m.totalProcessingTime += duration

	if duration < m.minProcessingTime {
		m.minProcessingTime = duration
	}
	if duration > m.maxProcessingTime {
		m.maxProcessingTime = duration
	}

	m.taskProcessingDurations = append(m.taskProcessingDurations, duration)
	if len(m.taskProcessingDurations) > m.maxStoredDurations {
		m.taskProcessingDurations = m.taskProcessingDurations[1:]
	}
}

func (m *Metrics) GetMetrics() (int, int, int, []time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.totalTasks, m.successfulTasks, m.failedTasks, m.taskProcessingDurations
}

func (m *Metrics) GetSummaryStats() map[string]interface{} {
	m.mu.Lock()
	defer m.mu.Unlock()

	stats := map[string]interface{}{
		"totalTasks":           m.totalTasks,
		"successfulTasks":      m.successfulTasks,
		"failedTasks":          m.failedTasks,
		"successRate":          0.0,
		"avgProcessingTime":    time.Duration(0),
		"minProcessingTime":    m.minProcessingTime,
		"maxProcessingTime":    m.maxProcessingTime,
		"medianProcessingTime": time.Duration(0),
		"p95ProcessingTime":    time.Duration(0),
	}

	if m.totalTasks > 0 {
		stats["successRate"] = float64(m.successfulTasks) / float64(m.totalTasks)
	}

	if m.totalTasks > 0 {
		stats["avgProcessingTime"] = m.totalProcessingTime / time.Duration(m.totalTasks)
	}

	durationsCount := len(m.taskProcessingDurations)
	if durationsCount > 0 {
		sortedDurations := make([]time.Duration, durationsCount)
		copy(sortedDurations, m.taskProcessingDurations)
		sort.Slice(sortedDurations, func(i, j int) bool {
			return sortedDurations[i] < sortedDurations[j]
		})

		medianIndex := durationsCount / 2
		stats["medianProcessingTime"] = sortedDurations[medianIndex]

		p95Index := int(float64(durationsCount) * 0.95)
		if p95Index >= durationsCount {
			p95Index = durationsCount - 1
		}
		stats["p95ProcessingTime"] = sortedDurations[p95Index]
	}

	return stats
}
