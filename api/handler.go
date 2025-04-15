package api

import (
	"distributed-task-queue/internal/metrics"
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
	Metrics   *metrics.Metrics
}

func NewTaskHandler(taskQueue *queue.Queue, log *logger.Logger, metrics *metrics.Metrics) *TaskHandler {
	return &TaskHandler{
		TaskQueue: taskQueue,
		Logger:    log,
		Metrics:   metrics,
	}
}

func (h *TaskHandler) AddTask(w http.ResponseWriter, r *http.Request) {
	var task queue.Task
	if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
		h.Logger.Error("Failed to decode task: " + err.Error())
		http.Error(w, "Invalid task data", http.StatusBadRequest)
		return
	}

	task.ID = uuid.New().String()
	task.Status = "pending"
	task.CreatedAt = time.Now()

	if err := h.TaskQueue.AddTask(task); err != nil {
		h.Logger.Error("Failed to add task to queue: " + err.Error())
		http.Error(w, "Failed to add task", http.StatusInternalServerError)
		return
	}

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

func (h *TaskHandler) GetMetrics(w http.ResponseWriter, r *http.Request) {
	stats := h.Metrics.GetSummaryStats()

	h.Logger.Info("Metrics requested")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

func (h *TaskHandler) GetTaskDetails(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	taskID := vars["id"]

	task, err := h.TaskQueue.GetTaskDetails(taskID)
	if err != nil {
		h.Logger.Warnf("Task not found: ID=%s", taskID)
		http.Error(w, "Task not found", http.StatusNotFound)
		return
	}

	h.Logger.Infof("Task details requested: ID=%s", taskID)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(task)
}

func (h *TaskHandler) ListDeadLetterTasks(w http.ResponseWriter, r *http.Request) {
	dlq := h.TaskQueue.GetDeadLetterQueue()
	tasks, err := dlq.ListTasks()

	if err != nil {
		h.Logger.Error("Failed to retrieve dead-letter tasks: " + err.Error())
		http.Error(w, "Failed to retrieve dead-letter tasks", http.StatusInternalServerError)
		return
	}

	h.Logger.Info("Dead-letter task list requested")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tasks)
}

func (h *TaskHandler) RetryDeadLetterTask(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	taskID := vars["id"]

	dlq := h.TaskQueue.GetDeadLetterQueue()
	err := dlq.RetryTask(taskID, h.TaskQueue)

	if err != nil {
		h.Logger.Warnf("Failed to retry dead-letter task: %s - %v", taskID, err)
		http.Error(w, "Failed to retry task: "+err.Error(), http.StatusBadRequest)
		return
	}

	h.Logger.Infof("Dead-letter task moved for retry: ID=%s", taskID)
	w.WriteHeader(http.StatusOK)
	response := map[string]string{"status": "Task moved back to main queue for processing"}
	json.NewEncoder(w).Encode(response)
}
