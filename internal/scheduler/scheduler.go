package scheduler

import (
	"time"
)

type Task struct {
	ID        string
	ExecuteAt time.Time
	Function  func() error
}

type Scheduler struct {
	tasks []Task
}

func NewScheduler() *Scheduler {
	return &Scheduler{
		tasks: make([]Task, 0),
	}
}

func (s *Scheduler) Schedule(task Task) {
	s.tasks = append(s.tasks, task)
}

func (s *Scheduler) Run() {
	for {
		now := time.Now()
		for _, task := range s.tasks {
			if task.ExecuteAt.Before(now) || task.ExecuteAt.Equal(now) {
				go func(t Task) {
					err := t.Function()
					if err != nil {

					}
				}(task)
			}
		}
		time.Sleep(time.Second)
	}
}
