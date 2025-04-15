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
}

func NewScheduler(taskQueue *queue.Queue, taskChannel chan *queue.Task, logger *logger.Logger) *Scheduler {
	return &Scheduler{
		taskQueue:   taskQueue,
		taskChannel: taskChannel,
		stopChan:    make(chan struct{}),
		logger:      logger,
	}
}

func (s *Scheduler) Start() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	go func() {
		for {
			select {
			case <-ticker.C:
				s.checkScheduledTasks()
			case <-s.stopChan:
				return
			}
		}
	}()
}

// checkScheduledTasks metodunu güncelleyin:

func (s *Scheduler) checkScheduledTasks() {
	now := time.Now()
	tasks := s.taskQueue.GetPendingTasks()

	for _, task := range tasks {
		if task.Status == "pending" && !task.ScheduledAt.After(now) {
			s.logger.Infof("Scheduled task is ready to run: %s", task.ID)

			// Tekrarlanan görev ise, bir sonraki çalıştırma için zamanla
			if task.IsRecurring {
				taskCopy := *task // Görevin bir kopyasını al

				// Orijinal görevi güncelle
				task.NextRun = now.Add(task.Interval)
				task.ScheduledAt = task.NextRun
				s.logger.Infof("Recurring task %s rescheduled for %v", task.ID, task.NextRun)

				// Kopyayı işlensin diye gönder
				s.taskChannel <- &taskCopy
			} else {
				// Tek seferlik görev
				s.taskChannel <- task
			}
		}
	}
}

func (s *Scheduler) Stop() {
	close(s.stopChan)
}
