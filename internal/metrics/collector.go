package metrics

import (
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
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
	TaskDistributions       prometheus.Counter
	TaskDistributionSkipped prometheus.Counter
	TaskCompletions         prometheus.Counter
	RedisConnectionErrors   prometheus.Counter
	RedisReconnectAttempts  prometheus.Counter
	RedisReconnectSuccess   prometheus.Counter
}

func NewMetrics() *Metrics {
	taskDistributions := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "task_queue_distributions_total",
		Help: "Total number of task distributions",
	})

	taskDistributionSkipped := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "task_queue_distributions_skipped_total",
		Help: "Total number of task distributions skipped",
	})

	taskCompletions := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "task_queue_completions_total",
		Help: "Total number of task completions",
	})

	redisConnectionErrors := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "task_queue_redis_connection_errors_total",
		Help: "Total number of Redis connection errors",
	})

	redisReconnectAttempts := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "task_queue_redis_reconnect_attempts_total",
		Help: "Total number of Redis reconnect attempts",
	})

	redisReconnectSuccess := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "task_queue_redis_reconnect_success_total",
		Help: "Total number of successful Redis reconnections",
	})

	prometheus.MustRegister(taskDistributions)
	prometheus.MustRegister(taskDistributionSkipped)
	prometheus.MustRegister(taskCompletions)
	prometheus.MustRegister(redisConnectionErrors)
	prometheus.MustRegister(redisReconnectAttempts)
	prometheus.MustRegister(redisReconnectSuccess)

	return &Metrics{
		taskProcessingDurations: make([]time.Duration, 0),
		maxStoredDurations:      1000,
		minProcessingTime:       time.Duration(1<<63 - 1),
		TaskDistributions:       taskDistributions,
		TaskDistributionSkipped: taskDistributionSkipped,
		TaskCompletions:         taskCompletions,
		RedisConnectionErrors:   redisConnectionErrors,
		RedisReconnectAttempts:  redisReconnectAttempts,
		RedisReconnectSuccess:   redisReconnectSuccess,
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
