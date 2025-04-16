package scheduler

import (
	"distributed-task-queue/internal/queue"
	"distributed-task-queue/pkg/logger"
	"sync"
	"time"
)

type Scheduler struct {
	taskQueue   *queue.Queue
	taskChannel chan *queue.Task
	mu          sync.Mutex
	stopChan    chan struct{}
	logger      *logger.Logger
	interval    time.Duration
}

func NewScheduler(taskQueue *queue.Queue, taskChannel chan *queue.Task, logger *logger.Logger, interval time.Duration) *Scheduler {
	if interval <= 0 {
		interval = 1 * time.Second
	}

	return &Scheduler{
		taskQueue:   taskQueue,
		taskChannel: taskChannel,
		stopChan:    make(chan struct{}),
		logger:      logger,
		interval:    interval,
	}
}

func (s *Scheduler) Start() {
	s.logger.Infof("Starting scheduler with interval: %v", s.interval)
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	go func() {
		for {
			select {
			case <-ticker.C:
				s.checkScheduledTasks()
			case <-s.stopChan:
				s.logger.Info("Scheduler stopped")
				return
			}
		}
	}()
}

func (s *Scheduler) checkScheduledTasks() {
	now := time.Now()
	tasks := s.taskQueue.GetPendingTasks()

	for _, task := range tasks {
		if task.Status == queue.TaskStatusScheduled && !task.ScheduledAt.After(now) {
			s.logger.Infof("Scheduled task is ready to run: %s", task.ID)

			task.Status = queue.TaskStatusPending
			task.AddEvent(queue.TaskStatusPending, "Scheduled time reached, task is ready for processing", "")

			if task.IsRecurring {
				s.taskChannel <- task

				nextTask := *task
				nextTask.ID = task.ID + "-next"
				nextTask.CreatedAt = time.Now()
				nextTask.StartedAt = time.Time{}
				nextTask.CompletedAt = time.Time{}
				nextTask.Status = queue.TaskStatusScheduled
				nextTask.RetryCount = 0
				nextTask.NextRun = time.Now().Add(task.Interval)
				nextTask.ScheduledAt = nextTask.NextRun

				s.logger.Infof("Re-scheduling recurring task for next run: %s at %v",
					nextTask.ID, nextTask.ScheduledAt)

				s.taskQueue.Enqueue(&nextTask)
			} else {
				s.taskChannel <- task
			}
		}
	}
}

func (s *Scheduler) SetInterval(interval time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if interval <= 0 {
		return
	}

	s.interval = interval
	s.logger.Infof("Scheduler interval updated: %v", interval)
}

func (s *Scheduler) Stop() {
	close(s.stopChan)
}
