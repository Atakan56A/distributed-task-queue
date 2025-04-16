package queue

import (
	"time"
)

type TaskStatus string

const (
	TaskStatusPending      TaskStatus = "pending"
	TaskStatusScheduled    TaskStatus = "scheduled"
	TaskStatusProcessing   TaskStatus = "processing"
	TaskStatusCompleted    TaskStatus = "completed"
	TaskStatusFailed       TaskStatus = "failed"
	TaskStatusDeadLettered TaskStatus = "dead-lettered"
	TaskStatusCancelled    TaskStatus = "cancelled"
)

type TaskEvent struct {
	Timestamp time.Time
	Status    TaskStatus
	Message   string
	Error     string
}

type TaskPriority int

const (
	PriorityLow    TaskPriority = 0
	PriorityNormal TaskPriority = 50
	PriorityHigh   TaskPriority = 100
)

type Task struct {
	ID          string
	Payload     interface{}
	Status      TaskStatus
	RetryCount  int
	MaxRetries  int
	Parameters  map[string]string
	CreatedAt   time.Time
	StartedAt   time.Time
	CompletedAt time.Time
	ScheduledAt time.Time
	IsRecurring bool
	Interval    time.Duration
	NextRun     time.Time
	Timeout     time.Duration
	LastError   string
	History     []TaskEvent
	Tags        []string
	Priority    TaskPriority
	CancelledAt time.Time
	CancelledBy string
	Result      interface{}
}

func NewTask(id string, payload interface{}, parameters map[string]string) *Task {
	now := time.Now()
	return &Task{
		ID:         id,
		Payload:    payload,
		Status:     TaskStatusPending,
		Parameters: parameters,
		CreatedAt:  now,
		MaxRetries: 3,
		History: []TaskEvent{
			{
				Timestamp: now,
				Status:    TaskStatusPending,
				Message:   "Task created",
			},
		},
	}
}

func (t *Task) ScheduleFor(scheduledTime time.Time) *Task {
	t.ScheduledAt = scheduledTime
	t.Status = TaskStatusScheduled

	t.AddEvent(TaskStatusScheduled, "Task scheduled for future execution", "")
	return t
}

func NewRecurringTask(id string, payload interface{}, parameters map[string]string, interval time.Duration) *Task {
	task := NewTask(id, payload, parameters)
	task.IsRecurring = true
	task.Interval = interval
	task.NextRun = time.Now().Add(interval)

	task.AddEvent(TaskStatusScheduled, "Recurring task created", "")
	return task
}

func (t *Task) SetTimeout(timeout time.Duration) *Task {
	t.Timeout = timeout
	return t
}

func (t *Task) AddEvent(status TaskStatus, message string, errorMessage string) {
	event := TaskEvent{
		Timestamp: time.Now(),
		Status:    status,
		Message:   message,
		Error:     errorMessage,
	}

	t.History = append(t.History, event)
	t.Status = status

	if errorMessage != "" {
		t.LastError = errorMessage
	}
}

func (t *Task) SetDeadLettered(reason string) {
	t.Status = TaskStatusDeadLettered
	t.AddEvent(TaskStatusDeadLettered, "Task moved to dead-letter queue", reason)
}

func (t *Task) SetFailed(errorMessage string) {
	t.Status = TaskStatusFailed
	t.AddEvent(TaskStatusFailed, "Task execution failed", errorMessage)
}

func (t *Task) SetCompleted(message string, storage Storage) {
	t.Status = TaskStatusCompleted
	t.CompletedAt = time.Now()
	t.AddEvent(TaskStatusCompleted, message, "")

	if storage != nil {
		storage.MarkTaskCompleted(t.ID)
	}
}

func (t *Task) SetProcessing() {
	t.Status = TaskStatusProcessing
	t.StartedAt = time.Now()
	t.AddEvent(TaskStatusProcessing, "Task processing started", "")
}

func (t *Task) SetCancelled(cancelledBy, reason string) {
	t.Status = TaskStatusCancelled
	t.CancelledAt = time.Now()
	t.CancelledBy = cancelledBy
	t.AddEvent(TaskStatusCancelled, reason, "")
}

func (t *Task) IsComplete() bool {
	return t.Status == TaskStatusCompleted ||
		t.Status == TaskStatusFailed ||
		t.Status == TaskStatusDeadLettered ||
		t.Status == TaskStatusCancelled
}

func (t *Task) SetPriority(priority TaskPriority) *Task {
	t.Priority = priority
	return t
}
