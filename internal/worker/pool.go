package worker

import (
	"context"
	"sync"
	"time"

	"distributed-task-queue/internal/metrics"
	"distributed-task-queue/internal/queue"
	"distributed-task-queue/pkg/logger"
)

type WorkerPool struct {
	workers            []*Worker
	taskQueue          chan *queue.Task
	wg                 sync.WaitGroup
	metrics            *metrics.Metrics
	queue              *queue.Queue
	webhookNotifier    WebhookNotifier
	mu                 sync.RWMutex
	minWorkers         int
	maxWorkers         int
	currentWorkers     int
	autoScaleEnabled   bool
	scaleUpThreshold   float64
	scaleDownThreshold float64
	scaleCheckInterval time.Duration
	ctx                context.Context
	cancel             context.CancelFunc
	logger             *logger.Logger
	nextWorkerID       int
}

func NewDynamicWorkerPool(
	initialWorkers, minWorkers, maxWorkers int,
	taskQueue chan *queue.Task,
	metrics *metrics.Metrics,
	queue *queue.Queue,
	webhookNotifier WebhookNotifier,
	logger *logger.Logger) *WorkerPool {

	if initialWorkers < minWorkers {
		initialWorkers = minWorkers
	}
	if initialWorkers > maxWorkers {
		initialWorkers = maxWorkers
	}

	ctx, cancel := context.WithCancel(context.Background())

	pool := &WorkerPool{
		workers:            make([]*Worker, 0, maxWorkers),
		taskQueue:          taskQueue,
		metrics:            metrics,
		queue:              queue,
		webhookNotifier:    webhookNotifier,
		minWorkers:         minWorkers,
		maxWorkers:         maxWorkers,
		currentWorkers:     0,
		autoScaleEnabled:   true,
		scaleUpThreshold:   0.7, // %70 kuyruk doluluk oranında ölçeklendirme
		scaleDownThreshold: 0.3, // %30 worker kullanım oranında küçültme
		scaleCheckInterval: 30 * time.Second,
		ctx:                ctx,
		cancel:             cancel,
		logger:             logger,
		nextWorkerID:       1,
	}

	pool.addWorkers(initialWorkers)

	if pool.autoScaleEnabled {
		go pool.autoScaleWorkers()
	}

	return pool
}

func (p *WorkerPool) addWorkers(count int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.currentWorkers >= p.maxWorkers {
		return
	}

	toAdd := count
	if p.currentWorkers+toAdd > p.maxWorkers {
		toAdd = p.maxWorkers - p.currentWorkers
	}

	p.logger.Infof("Adding %d workers to the pool", toAdd)

	for i := 0; i < toAdd; i++ {
		worker := NewWorker(p.nextWorkerID, p.taskQueue, p.metrics, p.queue, p.webhookNotifier)
		p.workers = append(p.workers, worker)
		p.nextWorkerID++
		p.currentWorkers++

		worker.Start()
	}
}

func (p *WorkerPool) removeWorkers(count int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.currentWorkers <= p.minWorkers {
		return
	}

	toRemove := count
	if p.currentWorkers-toRemove < p.minWorkers {
		toRemove = p.currentWorkers - p.minWorkers
	}

	p.logger.Infof("Removing %d workers from the pool", toRemove)

	removed := 0
	for i := len(p.workers) - 1; i >= 0 && removed < toRemove; i-- {
		if p.workers[i].IsIdle() {
			p.workers[i].Stop()
			p.workers = append(p.workers[:i], p.workers[i+1:]...)
			p.currentWorkers--
			removed++
		}
	}

	if removed < toRemove {
		p.logger.Warnf("Could only remove %d of %d workers (not enough idle workers)", removed, toRemove)
	}
}

func (p *WorkerPool) autoScaleWorkers() {
	ticker := time.NewTicker(p.scaleCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.checkAndAdjustWorkers()
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *WorkerPool) checkAndAdjustWorkers() {
	p.mu.RLock()
	queueSize := len(p.taskQueue)
	queueCapacity := cap(p.taskQueue)
	currentWorkers := p.currentWorkers
	p.mu.RUnlock()

	queueUtilization := float64(queueSize) / float64(queueCapacity)

	workerUtilization := p.getAverageWorkerUtilization()

	p.logger.Infof("Auto-scaling check: Queue=%d/%d (%.1f%%), Workers=%d, Utilization=%.1f%%",
		queueSize, queueCapacity, queueUtilization*100, currentWorkers, workerUtilization*100)

	if queueUtilization >= p.scaleUpThreshold && currentWorkers < p.maxWorkers {

		workersToAdd := 1
		if queueUtilization > 0.9 {

			workersToAdd = 2
		}

		p.logger.Infof("Scaling up due to high queue utilization (%.1f%%)", queueUtilization*100)
		p.addWorkers(workersToAdd)

	} else if workerUtilization <= p.scaleDownThreshold && currentWorkers > p.minWorkers {

		workersToRemove := 1
		p.logger.Infof("Scaling down due to low worker utilization (%.1f%%)", workerUtilization*100)
		p.removeWorkers(workersToRemove)
	}
}

func (p *WorkerPool) getAverageWorkerUtilization() float64 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.workers) == 0 {
		return 0.0
	}

	totalUtilization := 0.0
	for _, worker := range p.workers {
		totalUtilization += worker.GetUtilization()
	}

	return totalUtilization / float64(len(p.workers))
}

func (p *WorkerPool) SetAutoScaling(enabled bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	wasEnabled := p.autoScaleEnabled
	p.autoScaleEnabled = enabled

	if enabled && !wasEnabled {

		go p.autoScaleWorkers()
		p.logger.Info("Auto-scaling enabled")
	} else if !enabled && wasEnabled {
		p.logger.Info("Auto-scaling disabled")
	}
}

func (p *WorkerPool) SetScaleThresholds(upThreshold, downThreshold float64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if upThreshold > 0 && upThreshold <= 1.0 {
		p.scaleUpThreshold = upThreshold
	}

	if downThreshold >= 0 && downThreshold < upThreshold {
		p.scaleDownThreshold = downThreshold
	}

	p.logger.Infof("Scale thresholds updated: up=%.1f%%, down=%.1f%%",
		p.scaleUpThreshold*100, p.scaleDownThreshold*100)
}

func (p *WorkerPool) SetWorkerLimits(min, max int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if min > 0 && min <= max {
		oldMin := p.minWorkers
		p.minWorkers = min

		if p.currentWorkers < min {
			p.addWorkers(min - p.currentWorkers)
		}

		p.logger.Infof("Minimum workers updated: %d -> %d", oldMin, min)
	}

	if max >= min {
		oldMax := p.maxWorkers
		p.maxWorkers = max

		if p.currentWorkers > max {
			p.removeWorkers(p.currentWorkers - max)
		}

		p.logger.Infof("Maximum workers updated: %d -> %d", oldMax, max)
	}
}

func (p *WorkerPool) GetWorkerCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.currentWorkers
}

func (p *WorkerPool) GetWorkerStatus() []map[string]interface{} {
	p.mu.RLock()
	defer p.mu.RUnlock()

	result := make([]map[string]interface{}, len(p.workers))
	for i, worker := range p.workers {
		wm := worker.WorkerMetrics

		result[i] = map[string]interface{}{
			"id":                    worker.ID,
			"status":                worker.Status,
			"tasksProcessed":        wm.TasksProcessed,
			"successfulTasks":       wm.SuccessfulTasks,
			"failedTasks":           wm.FailedTasks,
			"utilization":           wm.GetUtilization(),
			"averageProcessingTime": wm.AverageProcessingTime.String(),
			"idleTime":              wm.GetIdleTime().String(),
		}
	}

	return result
}

func (p *WorkerPool) Wait() {
	p.wg.Wait()
}

func (p *WorkerPool) Stop() {
	p.cancel()

	p.mu.Lock()
	defer p.mu.Unlock()

	for _, worker := range p.workers {
		worker.Stop()
	}

	p.logger.Info("All workers stopped")
}

func (p *WorkerPool) GetMetrics() *metrics.Metrics {
	return p.metrics
}

func (p *WorkerPool) GetMinWorkers() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.minWorkers
}

func (p *WorkerPool) GetMaxWorkers() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.maxWorkers
}

func (p *WorkerPool) SetMinWorkers(min int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if min > 0 && min <= p.maxWorkers {
		oldMin := p.minWorkers
		p.minWorkers = min

		if p.currentWorkers < min {
			p.addWorkers(min - p.currentWorkers)
		}

		p.logger.Infof("Minimum workers updated: %d -> %d", oldMin, min)
	}
}

func (p *WorkerPool) SetMaxWorkers(max int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if max >= p.minWorkers {
		oldMax := p.maxWorkers
		p.maxWorkers = max

		if p.currentWorkers > max {
			p.removeWorkers(p.currentWorkers - max)
		}

		p.logger.Infof("Maximum workers updated: %d -> %d", oldMax, max)
	}
}

func (p *WorkerPool) IsAutoScalingEnabled() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.autoScaleEnabled
}

func (p *WorkerPool) GetScaleUpThreshold() float64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.scaleUpThreshold
}

func (p *WorkerPool) GetScaleDownThreshold() float64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.scaleDownThreshold
}

func (p *WorkerPool) GetWorkers() []*Worker {
	p.mu.RLock()
	defer p.mu.RUnlock()

	workers := make([]*Worker, len(p.workers))
	copy(workers, p.workers)

	return workers
}

func (p *WorkerPool) AddWorkers(count int) {
	p.addWorkers(count)
}

func (p *WorkerPool) RemoveWorkers(count int) {
	p.removeWorkers(count)
}
