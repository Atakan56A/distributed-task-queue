package queue

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

type Queue struct {
	tasks           []*Task
	completedTasks  map[string]*Task
	mu              sync.Mutex
	cond            *sync.Cond
	storage         Storage
	deadLetterQueue *DeadLetterQueue
}

type Storage interface {
	SaveTask(task *Task) error
	GetTask(taskID string) (*Task, error)
	GetAllTasks() ([]*Task, error)
	GetTasksByStatus(status string) ([]*Task, error)
	Close() error
}

func NewQueue() *Queue {
	q := &Queue{
		tasks:          make([]*Task, 0),
		completedTasks: make(map[string]*Task),
	}
	q.cond = sync.NewCond(&q.mu)

	q.deadLetterQueue = NewDeadLetterQueue()

	return q
}

func (q *Queue) SetStorage(storage Storage) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.storage = storage

	q.deadLetterQueue.SetStorage(storage)
}

func (q *Queue) GetDeadLetterQueue() *DeadLetterQueue {
	return q.deadLetterQueue
}

func (q *Queue) MoveToDeadLetterQueue(taskID, reason string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	task, ok := q.completedTasks[taskID]
	if !ok {
		return fmt.Errorf("task %s not found", taskID)
	}

	if task.Status != TaskStatusFailed {
		return fmt.Errorf("only failed tasks can be moved to dead letter queue")
	}

	q.deadLetterQueue.AddTask(task, reason)

	return nil
}

func (q *Queue) RestoreTasks() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.storage == nil {
		return errors.New("no storage configured")
	}

	tasks, err := q.storage.GetAllTasks()
	if err != nil {
		return fmt.Errorf("failed to restore tasks: %w", err)
	}

	for _, task := range tasks {
		if task.Status == TaskStatusPending || task.Status == TaskStatusScheduled {

			q.tasks = append(q.tasks, task)
		} else if task.Status == TaskStatusDeadLettered {

			q.deadLetterQueue.AddTask(task, "Restored from storage")
		} else {

			q.completedTasks[task.ID] = task

			if task.Status == TaskStatusProcessing {

				taskCopy := *task
				taskCopy.RetryCount++ // Yeniden deneme sayısını artır
				taskCopy.AddEvent(TaskStatusPending, "Task requeued after system restart", "Processing was interrupted")
				q.tasks = append(q.tasks, &taskCopy)
			}
		}
	}

	fmt.Printf("Restored %d pending tasks, %d completed/failed tasks, and dead-lettered tasks\n",
		len(q.tasks), len(q.completedTasks))
	return nil
}

func (q *Queue) TaskFailed(id string, errorMessage string, moveToDeadLetter bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	task, ok := q.completedTasks[id]
	if !ok {
		return // Görev bulunamadı
	}

	task.SetFailed(errorMessage)

	if q.storage != nil {
		go func(t *Task) {
			if err := q.storage.SaveTask(t); err != nil {
				fmt.Printf("Error updating failed task in storage: %v\n", err)
			}
		}(task)
	}

	if moveToDeadLetter {
		reason := fmt.Sprintf("Task failed after %d attempts. Last error: %s",
			task.RetryCount, errorMessage)
		q.deadLetterQueue.AddTask(task, reason)
	}
}

func (q *Queue) GetTaskDetails(id string) (*Task, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if task, ok := q.completedTasks[id]; ok {
		return task, nil
	}

	for _, task := range q.tasks {
		if task.ID == id {
			return task, nil
		}
	}

	task, err := q.deadLetterQueue.GetTask(id)
	if err == nil && task != nil {
		return task, nil
	}

	if q.storage != nil {
		return q.storage.GetTask(id)
	}

	return nil, errors.New("task not found")
}

func (q *Queue) Enqueue(task *Task) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.tasks = append(q.tasks, task)

	if q.storage != nil {
		go func(t *Task) {
			if err := q.storage.SaveTask(t); err != nil {
				fmt.Printf("Error saving task to storage: %v\n", err)
			}
		}(task)
	}

	q.cond.Signal()
}

func (q *Queue) Dequeue() *Task {
	q.mu.Lock()
	defer q.mu.Unlock()

	for len(q.tasks) == 0 {
		q.cond.Wait()
	}

	task := q.tasks[0]
	q.tasks = q.tasks[1:]

	q.completedTasks[task.ID] = task

	if q.storage != nil {
		go func(t *Task) {
			if err := q.storage.SaveTask(t); err != nil {
				fmt.Printf("Error updating task in storage: %v\n", err)
			}
		}(task)
	}

	return task
}

func (q *Queue) IsEmpty() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.tasks) == 0
}

func (q *Queue) AddTask(task Task) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	for _, t := range q.tasks {
		if t.ID == task.ID {
			return errors.New("task with this ID already exists")
		}
	}

	if _, exists := q.completedTasks[task.ID]; exists {
		return errors.New("task with this ID already exists in completed tasks")
	}

	taskCopy := &Task{
		ID:          task.ID,
		Payload:     task.Payload,
		Status:      task.Status,
		RetryCount:  task.RetryCount,
		Parameters:  task.Parameters,
		CreatedAt:   task.CreatedAt,
		StartedAt:   task.StartedAt,
		CompletedAt: task.CompletedAt,
	}

	q.tasks = append(q.tasks, taskCopy)

	if q.storage != nil {
		go func(t *Task) {
			if err := q.storage.SaveTask(t); err != nil {
				fmt.Printf("Error saving task to storage: %v\n", err)
			}
		}(taskCopy)
	}

	q.cond.Signal()
	return nil
}

func (q *Queue) GetTaskStatus(id string) (string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if task, ok := q.completedTasks[id]; ok {
		return string(task.Status), nil
	}

	for _, task := range q.tasks {
		if task.ID == id {
			return string(task.Status), nil
		}
	}

	if q.storage != nil {
		task, err := q.storage.GetTask(id)
		if err == nil && task != nil {
			return string(task.Status), nil
		}
	}

	return "", errors.New("task not found")
}

func (q *Queue) TaskCompleted(id string, status string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if task, ok := q.completedTasks[id]; ok {
		task.Status = TaskStatus(status)
		task.CompletedAt = time.Now()

		if q.storage != nil {
			go func(t *Task) {
				if err := q.storage.SaveTask(t); err != nil {
					fmt.Printf("Error updating task status in storage: %v\n", err)
				}
			}(task)
		}
	}
}

func (q *Queue) ListTasks(statusFilter string) ([]*Task, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	var result []*Task

	if statusFilter == "" || statusFilter == "pending" || statusFilter == "scheduled" {
		for _, task := range q.tasks {
			if statusFilter == "" || string(task.Status) == statusFilter {
				result = append(result, task)
			}
		}
	}

	for _, task := range q.completedTasks {
		if statusFilter == "" || string(task.Status) == statusFilter {
			result = append(result, task)
		}
	}

	if q.storage != nil {
		var storageTasks []*Task
		var err error

		if statusFilter == "" {
			storageTasks, err = q.storage.GetAllTasks()
		} else {
			storageTasks, err = q.storage.GetTasksByStatus(statusFilter)
		}

		if err != nil {
			return nil, fmt.Errorf("error retrieving tasks from storage: %w", err)
		}

		taskMap := make(map[string]bool)
		for _, task := range result {
			taskMap[task.ID] = true
		}

		for _, task := range storageTasks {
			if !taskMap[task.ID] {
				result = append(result, task)
			}
		}
	}

	return result, nil
}

func (q *Queue) GetPendingTasks() []*Task {
	q.mu.Lock()
	defer q.mu.Unlock()

	pendingTasks := make([]*Task, len(q.tasks))
	copy(pendingTasks, q.tasks)

	if q.storage != nil {
		taskMap := make(map[string]bool)
		for _, task := range q.tasks {
			taskMap[task.ID] = true
		}

		storageTasks, err := q.storage.GetTasksByStatus("pending")
		if err == nil {
			for _, task := range storageTasks {
				if !taskMap[task.ID] {
					pendingTasks = append(pendingTasks, task)
				}
			}
		}

		scheduledTasks, err := q.storage.GetTasksByStatus("scheduled")
		if err == nil {
			for _, task := range scheduledTasks {
				if !taskMap[task.ID] {
					pendingTasks = append(pendingTasks, task)
				}
			}
		}
	}

	return pendingTasks
}
