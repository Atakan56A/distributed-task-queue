package storage

import (
	"distributed-task-queue/internal/queue"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// FileStorage implements Storage interface using JSON files
type FileStorage struct {
	baseDir   string
	tasksFile string
	mu        sync.RWMutex
	tasks     map[string]*queue.Task
	saveTimer *time.Timer
}

// NewFileStorage creates a new file-based storage
func NewFileStorage(directory string) (*FileStorage, error) {
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage directory: %w", err)
	}

	fs := &FileStorage{
		baseDir:   directory,
		tasksFile: filepath.Join(directory, "tasks.json"),
		tasks:     make(map[string]*queue.Task),
	}

	// Try to load existing tasks
	if err := fs.loadTasks(); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to load existing tasks: %w", err)
	}

	// Set up periodic saving (every 5 seconds)
	fs.startAutoSave(5 * time.Second)

	return fs, nil
}

func (fs *FileStorage) startAutoSave(interval time.Duration) {
	fs.saveTimer = time.AfterFunc(interval, func() {
		fs.mu.RLock()
		taskCount := len(fs.tasks)
		fs.mu.RUnlock()

		if taskCount > 0 {
			if err := fs.saveTasks(); err != nil {
				fmt.Printf("Error auto-saving tasks: %v\n", err)
			}
		}

		// Reschedule the timer
		fs.saveTimer.Reset(interval)
	})
}

func (fs *FileStorage) loadTasks() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	data, err := os.ReadFile(fs.tasksFile)
	if err != nil {
		return err
	}

	var tasks []*queue.Task
	if err := json.Unmarshal(data, &tasks); err != nil {
		return fmt.Errorf("failed to parse tasks file: %w", err)
	}

	// Rebuild the task map
	fs.tasks = make(map[string]*queue.Task, len(tasks))
	for _, task := range tasks {
		fs.tasks[task.ID] = task
	}

	return nil
}

func (fs *FileStorage) saveTasks() error {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	// Convert map to slice for JSON storage
	tasks := make([]*queue.Task, 0, len(fs.tasks))
	for _, task := range fs.tasks {
		tasks = append(tasks, task)
	}

	data, err := json.MarshalIndent(tasks, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal tasks: %w", err)
	}

	tempFile := fs.tasksFile + ".tmp"
	if err := os.WriteFile(tempFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write tasks file: %w", err)
	}

	// Atomically replace the old file with the new one
	if err := os.Rename(tempFile, fs.tasksFile); err != nil {
		return fmt.Errorf("failed to rename tasks file: %w", err)
	}

	return nil
}

func (fs *FileStorage) SaveTask(task *queue.Task) error {
	fs.mu.Lock()
	fs.tasks[task.ID] = task
	fs.mu.Unlock()
	return nil
}

func (fs *FileStorage) SaveTasks(tasks []*queue.Task) error {
	fs.mu.Lock()
	for _, task := range tasks {
		fs.tasks[task.ID] = task
	}
	fs.mu.Unlock()
	return nil
}

func (fs *FileStorage) GetTask(taskID string) (*queue.Task, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	task, exists := fs.tasks[taskID]
	if !exists {
		return nil, errors.New("task not found")
	}
	return task, nil
}

func (fs *FileStorage) GetAllTasks() ([]*queue.Task, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	tasks := make([]*queue.Task, 0, len(fs.tasks))
	for _, task := range fs.tasks {
		tasks = append(tasks, task)
	}
	return tasks, nil
}

func (fs *FileStorage) GetTasksByStatus(status string) ([]*queue.Task, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	var tasks []*queue.Task
	for _, task := range fs.tasks {
		if task.Status == status {
			tasks = append(tasks, task)
		}
	}
	return tasks, nil
}

func (fs *FileStorage) DeleteTask(taskID string) error {
	fs.mu.Lock()
	delete(fs.tasks, taskID)
	fs.mu.Unlock()
	return nil
}

func (fs *FileStorage) Close() error {
	// Stop the auto-save timer
	if fs.saveTimer != nil {
		fs.saveTimer.Stop()
	}

	// Final save
	return fs.saveTasks()
}
