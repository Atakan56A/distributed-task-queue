package queue

import (
	"fmt"
	"sync"
	"time"
)

type DeadLetterQueue struct {
	tasks   map[string]*Task
	mu      sync.RWMutex
	storage Storage
}

func NewDeadLetterQueue() *DeadLetterQueue {
	return &DeadLetterQueue{
		tasks: make(map[string]*Task),
	}
}

func (dlq *DeadLetterQueue) SetStorage(storage Storage) {
	dlq.mu.Lock()
	defer dlq.mu.Unlock()
	dlq.storage = storage
}

func (dlq *DeadLetterQueue) AddTask(task *Task, reason string) {
	dlq.mu.Lock()
	defer dlq.mu.Unlock()

	task.SetDeadLettered(reason)

	dlq.tasks[task.ID] = task

	if dlq.storage != nil {
		go func(t *Task) {
			if err := dlq.storage.SaveTask(t); err != nil {
				fmt.Printf("Error saving dead-lettered task to storage: %v\n", err)
			}
		}(task)
	}
}

func (dlq *DeadLetterQueue) GetTask(id string) (*Task, error) {
	dlq.mu.RLock()
	defer dlq.mu.RUnlock()

	task, exists := dlq.tasks[id]
	if !exists {

		if dlq.storage != nil {
			return dlq.storage.GetTask(id)
		}
		return nil, fmt.Errorf("task %s not found in dead letter queue", id)
	}

	return task, nil
}

func (dlq *DeadLetterQueue) ListTasks() ([]*Task, error) {
	dlq.mu.RLock()
	defer dlq.mu.RUnlock()

	tasks := make([]*Task, 0, len(dlq.tasks))
	for _, task := range dlq.tasks {
		tasks = append(tasks, task)
	}

	if dlq.storage != nil {
		storageTasks, err := dlq.storage.GetTasksByStatus(string(TaskStatusDeadLettered))
		if err != nil {
			return tasks, fmt.Errorf("error getting dead-lettered tasks from storage: %w", err)
		}

		taskMap := make(map[string]bool)
		for _, task := range tasks {
			taskMap[task.ID] = true
		}

		for _, task := range storageTasks {
			if !taskMap[task.ID] {
				tasks = append(tasks, task)
			}
		}
	}

	return tasks, nil
}

func (dlq *DeadLetterQueue) RetryTask(id string, queue *Queue) error {
	dlq.mu.Lock()
	defer dlq.mu.Unlock()

	task, exists := dlq.tasks[id]
	if !exists {

		if dlq.storage != nil {
			var err error
			task, err = dlq.storage.GetTask(id)
			if err != nil || task == nil {
				return fmt.Errorf("task %s not found in dead letter queue", id)
			}

			if task.Status != TaskStatusDeadLettered {
				return fmt.Errorf("task %s is not in dead-lettered status", id)
			}
		} else {
			return fmt.Errorf("task %s not found in dead letter queue", id)
		}
	}

	task.RetryCount = 0
	task.AddEvent(TaskStatusPending, "Task moved back from dead-letter queue for retry", "")

	delete(dlq.tasks, id)

	queue.Enqueue(task)

	return nil
}

func (dlq *DeadLetterQueue) PurgeOldTasks(olderThan time.Duration) int {
	dlq.mu.Lock()
	defer dlq.mu.Unlock()

	cutoff := time.Now().Add(-olderThan)
	removed := 0

	for id, task := range dlq.tasks {
		if task.CompletedAt.Before(cutoff) {
			delete(dlq.tasks, id)
			removed++
		}
	}

	return removed
}
