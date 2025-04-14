package worker

import (
	"context"
	"sync"

	"distributed-task-queue/internal/queue"
)

type WorkerPool struct {
	workers   []*Worker
	taskQueue chan *queue.Task
	wg        sync.WaitGroup
}

func NewWorkerPool(numWorkers int, taskQueue chan *queue.Task) *WorkerPool {
	pool := &WorkerPool{
		workers:   make([]*Worker, numWorkers),
		taskQueue: taskQueue,
	}

	ctx := context.Background()

	for i := 0; i < numWorkers; i++ {
		worker := NewWorker(i, taskQueue)
		pool.workers[i] = worker
		pool.wg.Add(1)
		go func(w *Worker) {
			defer pool.wg.Done()
			w.Start(ctx)
		}(worker)
	}

	return pool
}

func (p *WorkerPool) Wait() {
	p.wg.Wait()
}

func (p *WorkerPool) Stop() {
	for _, worker := range p.workers {
		worker.Stop()
	}
}
