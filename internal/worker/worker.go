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

func NewWorker(id int, taskChannel chan *queue.Task) *Worker {
	return &Worker{
		ID:          id,
		TaskChannel: taskChannel, // Artık kendi oluşturmak yerine dışarıdan alıyor
		Quit:        make(chan bool),
	}
}

func (w *Worker) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case task := <-w.TaskChannel:
				log.Printf("Worker processTask")
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
	log.Printf("Worker %d started processing task: %v\n", w.ID, task.ID)

	task.Status = "processing"
	task.StartedAt = time.Now()

	success := processTaskLogic(task)

	if !success {
		log.Printf("Worker %d encountered an issue with task: %v\n", w.ID, task.ID)
		task.Status = "failed"
	} else {
		log.Printf("Worker %d successfully completed task: %v\n", w.ID, task.ID)
		task.Status = "completed"
	}

	task.CompletedAt = time.Now()
}

func processTaskLogic(_ *queue.Task) bool {
	time.Sleep(2 * time.Second)
	return true
}

func (w *Worker) Stop() {
	w.Quit <- true
}
