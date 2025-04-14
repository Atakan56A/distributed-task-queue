package worker

import (
	"context"
	"log"
	"time"

	"distributed-task-queue/internal/queue"
)

type Worker struct {
	ID          int
	TaskChannel chan *queue.Task
	Quit        chan bool
}

func NewWorker(id int) *Worker {
	return &Worker{
		ID:          id,
		TaskChannel: make(chan *queue.Task),
		Quit:        make(chan bool),
	}
}

func (w *Worker) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case task := <-w.TaskChannel:
				w.processTask(task)
			case <-w.Quit:
				log.Printf("Worker %d stopping\n", w.ID)
				return
			case <-ctx.Done():
				log.Printf("Worker %d context done\n", w.ID)
				return
			}
		}
	}()
}

func (w *Worker) processTask(task *queue.Task) {
	log.Printf("Worker %d processing task: %v\n", w.ID, task.ID)

	task.Status = "processing"
	task.StartedAt = time.Now()

	time.Sleep(2 * time.Second)

	task.Status = "completed"
	task.CompletedAt = time.Now()

	log.Printf("Worker %d finished task: %v\n", w.ID, task.ID)
}

func (w *Worker) Stop() {
	w.Quit <- true
}
