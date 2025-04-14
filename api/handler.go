package api

import (
	"distributed-task-queue/internal/queue"
	"distributed-task-queue/pkg/logger"
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

type TaskHandler struct {
	TaskQueue *queue.Queue
	Logger    *logger.Logger
}

func NewTaskHandler(taskQueue *queue.Queue, log *logger.Logger) *TaskHandler {
	return &TaskHandler{TaskQueue: taskQueue, Logger: log}
}

func (h *TaskHandler) AddTask(w http.ResponseWriter, r *http.Request) {
	var task queue.Task
	if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
		h.Logger.Error("Failed to decode task: " + err.Error())
		http.Error(w, "Invalid task data", http.StatusBadRequest)
		return
	}

	// Set task fields
	task.ID = uuid.New().String()
	task.Status = "pending"
	task.CreatedAt = time.Now()

	if err := h.TaskQueue.AddTask(task); err != nil {
		h.Logger.Error("Failed to add task to queue: " + err.Error())
		http.Error(w, "Failed to add task", http.StatusInternalServerError)
		return
	}

	// Log the task addition
	h.Logger.Infof("Task added: ID=%s, Status=%s", task.ID, task.Status)
	w.WriteHeader(http.StatusCreated)
}

func (h *TaskHandler) GetTaskStatus(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	taskID := vars["id"]

	status, err := h.TaskQueue.GetTaskStatus(taskID)
	if err != nil {
		h.Logger.Warnf("Task not found: ID=%s", taskID)
		http.Error(w, "Task not found", http.StatusNotFound)
		return
	}

	response := map[string]string{"id": taskID, "status": status}
	h.Logger.Infof("Task status requested: ID=%s, Status=%s", taskID, status)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (h *TaskHandler) ListTasks(w http.ResponseWriter, r *http.Request) {
	statusFilter := r.URL.Query().Get("status")

	tasks, err := h.TaskQueue.ListTasks(statusFilter)
	if err != nil {
		h.Logger.Error("Failed to retrieve tasks: " + err.Error())
		http.Error(w, "Failed to retrieve tasks", http.StatusInternalServerError)
		return
	}

	h.Logger.Infof("Task list requested with filter: %s", statusFilter)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tasks)
}
