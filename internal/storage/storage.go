package storage

import (
	"distributed-task-queue/internal/queue"
	"time"
)

type Storage interface {
	SaveTask(task *queue.Task) error

	SaveTasks(tasks []*queue.Task) error

	GetTask(taskID string) (*queue.Task, error)

	GetAllTasks() ([]*queue.Task, error)

	GetTasksByStatus(status string) ([]*queue.Task, error)

	DeleteTask(taskID string) error

	Close() error

	IsTaskCompleted(taskID string) (bool, error)
	MarkTaskCompleted(taskID string) error
	AcquireTaskLock(taskID, nodeID string, timeout time.Duration) (bool, error)
	ReleaseTaskLock(taskID, nodeID string) error
}
