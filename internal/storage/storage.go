package storage

import (
	"distributed-task-queue/internal/queue"
)

// Storage is an interface for persisting and retrieving tasks
type Storage interface {
	// SaveTask saves a task to storage
	SaveTask(task *queue.Task) error

	// SaveTasks saves multiple tasks to storage at once
	SaveTasks(tasks []*queue.Task) error

	// GetTask retrieves a task by ID
	GetTask(taskID string) (*queue.Task, error)

	// GetAllTasks retrieves all tasks from storage
	GetAllTasks() ([]*queue.Task, error)

	// GetTasksByStatus retrieves tasks with a specific status
	GetTasksByStatus(status string) ([]*queue.Task, error)

	// DeleteTask deletes a task from storage
	DeleteTask(taskID string) error

	// Close closes the storage connection
	Close() error
}
