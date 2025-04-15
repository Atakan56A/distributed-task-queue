package queue

import (
	"fmt"
	"sync"
	"time"
)

type Queue struct {
	tasks          []*Task
	completedTasks map[string]*Task
	mu             sync.Mutex
	cond           *sync.Cond
}

func NewQueue() *Queue {
	q := &Queue{
		tasks:          make([]*Task, 0),
		completedTasks: make(map[string]*Task),
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *Queue) Enqueue(task *Task) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.tasks = append(q.tasks, task)
	q.cond.Signal()
}

func (q *Queue) Dequeue() *Task {
	q.mu.Lock()
	defer q.mu.Unlock()

	for len(q.tasks) == 0 {
		q.cond.Wait() // Pasif bekleme
	}

	task := q.tasks[0]
	q.tasks = q.tasks[1:]

	q.completedTasks[task.ID] = task

	return task
}

func (q *Queue) IsEmpty() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.tasks) == 0
}

func (q *Queue) AddTask(task Task) error {

	taskPtr := &Task{
		ID:          task.ID,
		Payload:     task.Payload,
		Status:      task.Status,
		RetryCount:  task.RetryCount,
		Parameters:  task.Parameters,
		CreatedAt:   task.CreatedAt,
		StartedAt:   task.StartedAt,
		CompletedAt: task.CompletedAt,
	}
	q.Enqueue(taskPtr)
	return nil
}

func (q *Queue) GetTaskStatus(id string) (string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for _, task := range q.tasks {
		if task.ID == id {
			return task.Status, nil
		}
	}

	if task, found := q.completedTasks[id]; found {
		return task.Status, nil
	}

	return "", fmt.Errorf("task with ID %s not found", id)
}

func (q *Queue) TaskCompleted(id string, status string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if task, found := q.completedTasks[id]; found {
		task.Status = status
		task.CompletedAt = time.Now()
	}
}

func (q *Queue) ListTasks(statusFilter string) ([]*Task, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	result := make([]*Task, 0)

	for _, task := range q.tasks {
		if statusFilter == "" || task.Status == statusFilter {
			result = append(result, task)
		}
	}

	for _, task := range q.completedTasks {
		if statusFilter == "" || task.Status == statusFilter {
			result = append(result, task)
		}
	}

	return result, nil
}

func (q *Queue) GetPendingTasks() []*Task {
	q.mu.Lock()
	defer q.mu.Unlock()

	var pendingTasks []*Task
	for _, task := range q.tasks {
		if task.Status == "pending" {
			pendingTasks = append(pendingTasks, task)
		}
	}
	return pendingTasks
}
