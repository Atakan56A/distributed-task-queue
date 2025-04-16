package worker

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"distributed-task-queue/internal/metrics"
	"distributed-task-queue/internal/queue"
	"distributed-task-queue/internal/retry"
)

type WebhookNotifier interface {
	NotifyTaskEvent(task interface{}, event string)
}

type Worker struct {
	ID              int
	TaskChannel     chan *queue.Task
	Quit            chan bool
	Metrics         *metrics.Metrics
	TaskQueue       *queue.Queue
	WebhookNotifier WebhookNotifier
	WorkerMetrics   *WorkerMetrics
	Status          WorkerStatus
	ctx             context.Context
	cancel          context.CancelFunc
}

type WorkerStatus string

const (
	WorkerStatusIdle     WorkerStatus = "idle"
	WorkerStatusBusy     WorkerStatus = "busy"
	WorkerStatusStopping WorkerStatus = "stopping"
	WorkerStatusStopped  WorkerStatus = "stopped"
)

func NewWorker(id int, taskChannel chan *queue.Task, metrics *metrics.Metrics,
	taskQueue *queue.Queue, webhookNotifier WebhookNotifier) *Worker {
	ctx, cancel := context.WithCancel(context.Background())

	return &Worker{
		ID:              id,
		TaskChannel:     taskChannel,
		Quit:            make(chan bool),
		Metrics:         metrics,
		TaskQueue:       taskQueue,
		WebhookNotifier: webhookNotifier,
		WorkerMetrics:   NewWorkerMetrics(),
		Status:          WorkerStatusIdle,
		ctx:             ctx,
		cancel:          cancel,
	}
}

func (w *Worker) Start() {
	go func() {
		for {
			select {
			case task := <-w.TaskChannel:
				w.Status = WorkerStatusBusy
				w.WorkerMetrics.RecordTaskStart()
				log.Printf("Worker %d processing task: %v\n", w.ID, task.ID)
				w.processTask(task)
				w.Status = WorkerStatusIdle

			case <-w.Quit:
				w.Status = WorkerStatusStopping
				log.Printf("Worker %d stopping\n", w.ID)
				w.Status = WorkerStatusStopped
				return

			case <-w.ctx.Done():
				w.Status = WorkerStatusStopping
				log.Printf("Worker %d context done\n", w.ID)
				w.Status = WorkerStatusStopped
				return
			}
		}
	}()
}

func (w *Worker) processTask(task *queue.Task) {
	log.Printf("Worker %d started processing task: %v\n", w.ID, task.ID)

	task.SetProcessing()
	startTime := time.Now()

	maxRetries := task.MaxRetries
	if maxRetries <= 0 {
		maxRetries = retry.MaxRetries
	}

	backoff := retry.NewExponentialBackoff()
	success := false
	var lastError string

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			log.Printf("Worker %d retrying task: %v (attempt %d/%d)\n", w.ID, task.ID, attempt, maxRetries)
			task.RetryCount++
			task.AddEvent(queue.TaskStatusProcessing, fmt.Sprintf("Retry attempt %d/%d", attempt, maxRetries), lastError)
		}

		timeout := 30 * time.Second
		if task.Timeout > 0 {
			timeout = task.Timeout
		}

		ctx, cancel := context.WithTimeout(context.Background(), timeout)

		resultCh := make(chan bool, 1)
		errorCh := make(chan string, 1)

		go func() {
			result, errMsg := processTaskLogic(task)
			resultCh <- result
			if !result {
				errorCh <- errMsg
			}
		}()

		select {
		case taskSuccess := <-resultCh:
			if taskSuccess {
				success = true
			} else {
				lastError = <-errorCh
				log.Printf("Worker %d task attempt failed: %v - %s\n", w.ID, task.ID, lastError)
			}
		case <-ctx.Done():
			lastError = "Task execution timed out"
			log.Printf("Worker %d task attempt timed out: %v\n", w.ID, task.ID)
			task.AddEvent(queue.TaskStatusFailed, "Task execution timed out", lastError)
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

	duration := time.Since(startTime)
	w.WorkerMetrics.RecordTaskCompletion(duration, success)

	if success {
		log.Printf("Worker %d successfully completed task: %v\n", w.ID, task.ID)
		task.SetCompleted("Task completed successfully")
		if w.Metrics != nil {
			w.Metrics.RecordTaskSuccess(duration)
		}

		if w.WebhookNotifier != nil {
			w.WebhookNotifier.NotifyTaskEvent(task, "completed")
		}
	} else {
		log.Printf("Worker %d failed to complete task after %d attempts: %v\n", w.ID, task.RetryCount+1, task.ID)
		task.SetFailed(lastError)

		if task.RetryCount >= maxRetries {
			log.Printf("Worker %d moving task to dead-letter queue: %v\n", w.ID, task.ID)
			reason := fmt.Sprintf("Failed after %d attempts. Last error: %s", task.RetryCount, lastError)
			if w.TaskQueue != nil {
				w.TaskQueue.MoveToDeadLetterQueue(task.ID, reason)
			}

			if w.WebhookNotifier != nil {
				w.WebhookNotifier.NotifyTaskEvent(task, "dead-lettered")
			}
		} else {
			if w.WebhookNotifier != nil {
				w.WebhookNotifier.NotifyTaskEvent(task, "failed")
			}
		}

		if w.Metrics != nil {
			w.Metrics.RecordTaskFailure(duration)
		}
	}
}

func processTaskLogic(_ *queue.Task) (bool, string) {
	time.Sleep(10 * time.Second)
	if rand.Intn(10) < 3 {
		return false, "Simulated random task failure"
	}
	return true, ""
}

func (w *Worker) Stop() {
	w.Status = WorkerStatusStopping
	w.cancel()
	w.Quit <- true
}

func (w *Worker) GetUtilization() float64 {
	return w.WorkerMetrics.GetUtilization()
}

func (w *Worker) IsIdle() bool {
	return w.Status == WorkerStatusIdle
}
