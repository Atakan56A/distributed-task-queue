package worker

import (
	"sync"
	"time"
)

type WorkerMetrics struct {
	TasksProcessed        int64
	SuccessfulTasks       int64
	FailedTasks           int64
	TotalProcessingTime   time.Duration
	AverageProcessingTime time.Duration
	LastActivityTime      time.Time
	CurrentlyProcessing   bool
	IdleTime              time.Duration
	mu                    sync.RWMutex
}

func NewWorkerMetrics() *WorkerMetrics {
	return &WorkerMetrics{
		LastActivityTime: time.Now(),
	}
}

func (wm *WorkerMetrics) RecordTaskStart() {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	now := time.Now()
	wm.IdleTime += now.Sub(wm.LastActivityTime)
	wm.LastActivityTime = now
	wm.CurrentlyProcessing = true
}

func (wm *WorkerMetrics) RecordTaskCompletion(duration time.Duration, success bool) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	wm.TasksProcessed++
	wm.TotalProcessingTime += duration
	wm.AverageProcessingTime = wm.TotalProcessingTime / time.Duration(wm.TasksProcessed)
	wm.CurrentlyProcessing = false
	wm.LastActivityTime = time.Now()

	if success {
		wm.SuccessfulTasks++
	} else {
		wm.FailedTasks++
	}
}

func (wm *WorkerMetrics) IsIdle() bool {
	wm.mu.RLock()
	defer wm.mu.RUnlock()
	return !wm.CurrentlyProcessing
}

func (wm *WorkerMetrics) GetIdleTime() time.Duration {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	if wm.CurrentlyProcessing {
		return 0
	}

	return time.Since(wm.LastActivityTime) + wm.IdleTime
}

func (wm *WorkerMetrics) GetUtilization() float64 {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	totalTime := wm.TotalProcessingTime + wm.IdleTime
	if totalTime == 0 {
		return 0.0
	}

	return float64(wm.TotalProcessingTime) / float64(totalTime)
}
