package worker

import (
	"distributed-task-queue/internal/queue"
	"sync"
	"time"
)

type TaskDistributor struct {
	pool                 *WorkerPool
	taskQueue            chan *queue.Task
	distributionStrategy DistributionStrategy
	mu                   sync.RWMutex
}

type DistributionStrategy string

const (
	StrategyRoundRobin   DistributionStrategy = "round-robin"   // Simple round-robin distribution
	StrategyLeastLoaded  DistributionStrategy = "least-loaded"  // Send to least loaded worker
	StrategyTaskAffinity DistributionStrategy = "task-affinity" // Workers specialize in certain task types
)

func NewTaskDistributor(pool *WorkerPool, taskQueue chan *queue.Task) *TaskDistributor {
	return &TaskDistributor{
		pool:                 pool,
		taskQueue:            taskQueue,
		distributionStrategy: StrategyLeastLoaded, // Default to least loaded strategy
	}
}

func (td *TaskDistributor) SetDistributionStrategy(strategy DistributionStrategy) {
	td.mu.Lock()
	defer td.mu.Unlock()
	td.distributionStrategy = strategy
}

func (td *TaskDistributor) findLeastLoadedWorker() *Worker {
	td.mu.RLock()
	defer td.mu.RUnlock()

	pool := td.pool
	pool.mu.RLock()
	defer pool.mu.RUnlock()

	if len(pool.workers) == 0 {
		return nil
	}

	var leastLoadedWorker *Worker
	lowestUtilization := 1.1 // Start with value higher than possible

	for _, worker := range pool.workers {
		if worker.Status == WorkerStatusStopping || worker.Status == WorkerStatusStopped {
			continue
		}

		if worker.Status == WorkerStatusIdle {
			return worker
		}

		utilization := worker.GetUtilization()
		if utilization < lowestUtilization {
			lowestUtilization = utilization
			leastLoadedWorker = worker
		}
	}

	return leastLoadedWorker
}

func (td *TaskDistributor) DistributeTasks(workerTaskChannels map[int]chan *queue.Task) {
	for task := range td.taskQueue {
		td.mu.RLock()
		strategy := td.distributionStrategy
		td.mu.RUnlock()

		switch strategy {
		case StrategyLeastLoaded:

			worker := td.findLeastLoadedWorker()
			if worker != nil {

				workerTaskChannels[worker.ID] <- task
			} else {

				td.taskQueue <- task               // Put back in the queue
				time.Sleep(100 * time.Millisecond) // Wait a bit before retrying
			}

		case StrategyTaskAffinity:

			var targetWorker *Worker

			if len(task.Tags) > 0 {

				targetTag := task.Tags[0]

				tagHash := 0
				for _, c := range targetTag {
					tagHash += int(c)
				}

				td.pool.mu.RLock()
				if len(td.pool.workers) > 0 {
					workerIndex := tagHash % len(td.pool.workers)
					targetWorker = td.pool.workers[workerIndex]
				}
				td.pool.mu.RUnlock()
			}

			if targetWorker != nil {
				workerTaskChannels[targetWorker.ID] <- task
			} else {

				worker := td.findLeastLoadedWorker()
				if worker != nil {
					workerTaskChannels[worker.ID] <- task
				} else {
					td.taskQueue <- task // Put back in the queue
					time.Sleep(100 * time.Millisecond)
				}
			}

		default: // StrategyRoundRobin or unknown
			td.taskQueue <- task
		}
	}
}
