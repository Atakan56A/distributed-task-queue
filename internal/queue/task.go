package queue

import "time"

type Task struct {
	ID          string
	Payload     interface{}
	Status      string
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
}

func NewTask(id string, payload interface{}, parameters map[string]string) *Task {
	return &Task{
		ID:          id,
		Payload:     payload,
		Status:      "pending",
		RetryCount:  0,
		MaxRetries:  3,
		Parameters:  parameters,
		CreatedAt:   time.Now(),
		ScheduledAt: time.Now(),
	}
}

func (t *Task) ScheduleFor(scheduledTime time.Time) *Task {
	t.ScheduledAt = scheduledTime
	return t
}

func NewRecurringTask(id string, payload interface{}, parameters map[string]string, interval time.Duration) *Task {
	task := NewTask(id, payload, parameters)
	task.IsRecurring = true
	task.Interval = interval
	task.NextRun = time.Now().Add(interval)
	return task
}

func (t *Task) SetTimeout(timeout time.Duration) *Task {
	t.Timeout = timeout
	return t
}
