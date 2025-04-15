package worker

import (
	"context"
	"log"
	"time"

	"distributed-task-queue/internal/queue"
	"distributed-task-queue/internal/retry"
)

type Worker struct {
	ID          int
	TaskChannel chan *queue.Task
	Quit        chan bool
}

func NewWorker(id int, taskChannel chan *queue.Task) *Worker {
	return &Worker{
		ID:          id,
		TaskChannel: taskChannel,
		Quit:        make(chan bool),
	}
}

func (w *Worker) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case task := <-w.TaskChannel:
				log.Printf("Worker processTask")
				w.processTask(task)
			case <-w.Quit:
				log.Printf("Worker %d stopping\n", w.ID)
				return
			case <-ctx.Done():
				log.Printf("Worker %d context done\n", w.ID)
				return
			}
		}
	}()
}

func (w *Worker) processTask(task *queue.Task) {
	log.Printf("Worker %d started processing task: %v\n", w.ID, task.ID)

	task.Status = "processing"
	task.StartedAt = time.Now()

	maxRetries := task.MaxRetries
	if maxRetries <= 0 {
		maxRetries = retry.MaxRetries
	}

	backoff := retry.NewExponentialBackoff()
	success := false

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			log.Printf("Worker %d retrying task: %v (attempt %d/%d)\n", w.ID, task.ID, attempt, maxRetries)
			task.RetryCount++
		}

		timeout := 30 * time.Second
		if task.Timeout > 0 {
			timeout = task.Timeout
		}

		ctx, cancel := context.WithTimeout(context.Background(), timeout)

		done := make(chan bool, 1)
		go func() {
			result := processTaskLogic(task)
			done <- result
		}()

		select {
		case taskSuccess := <-done:
			if taskSuccess {
				success = true
				cancel()
				break
			}
		case <-ctx.Done():
			log.Printf("Worker %d task attempt timed out: %v\n", w.ID, task.ID)
		}

		cancel()

		if success {
			break
		}

		if attempt < maxRetries {
			delay := backoff.GetDelay(attempt)
			log.Printf("Worker %d waiting %v before next retry for task: %v\n", w.ID, delay, task.ID)
			time.Sleep(delay)
		}
	}

	if success {
		log.Printf("Worker %d successfully completed task: %v\n", w.ID, task.ID)
		task.Status = "completed"
	} else {
		log.Printf("Worker %d failed to complete task after %d attempts: %v\n", w.ID, task.RetryCount+1, task.ID)
		task.Status = "failed"
	}

	task.CompletedAt = time.Now()
}

func processTaskLogic(_ *queue.Task) bool {
	time.Sleep(2 * time.Second)
	return true
}

func (w *Worker) Stop() {
	w.Quit <- true
}
