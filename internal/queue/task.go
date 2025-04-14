package queue

import "time"

type Task struct {
	ID          string
	Payload     interface{}
	Status      string
	RetryCount  int
	Parameters  map[string]string
	CreatedAt   time.Time
	StartedAt   time.Time
	CompletedAt time.Time
}

func NewTask(id string, payload interface{}, parameters map[string]string) *Task {
	return &Task{
		ID:         id,
		Payload:    payload,
		Status:     "pending",
		RetryCount: 0,
		Parameters: parameters,
		CreatedAt:  time.Now(),
	}
}
