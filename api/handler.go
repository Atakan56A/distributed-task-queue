package api

import (
	"distributed-task-queue/internal/metrics"
	"distributed-task-queue/internal/queue"
	"distributed-task-queue/internal/webhook"
	"distributed-task-queue/internal/worker"
	"distributed-task-queue/pkg/logger"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

type TaskHandler struct {
	TaskQueue       *queue.Queue
	Logger          *logger.Logger
	Metrics         *metrics.Metrics
	WebhookManager  *webhook.WebhookManager
	WorkerPool      *worker.WorkerPool
	TaskDistributor *worker.TaskDistributor
}

func NewTaskHandler(
	taskQueue *queue.Queue,
	log *logger.Logger,
	metrics *metrics.Metrics,
	webhookManager *webhook.WebhookManager,
	workerPool *worker.WorkerPool,
	taskDistributor *worker.TaskDistributor) *TaskHandler {

	return &TaskHandler{
		TaskQueue:       taskQueue,
		Logger:          log,
		Metrics:         metrics,
		WebhookManager:  webhookManager,
		WorkerPool:      workerPool,
		TaskDistributor: taskDistributor,
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

func (h *TaskHandler) CancelTask(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	taskID := vars["id"]

	var cancelInfo struct {
		Reason      string `json:"reason"`
		CancelledBy string `json:"cancelledBy"`
	}

	if err := json.NewDecoder(r.Body).Decode(&cancelInfo); err != nil {
		h.Logger.Warnf("Invalid cancel request: %v", err)
		http.Error(w, "Invalid cancel request", http.StatusBadRequest)
		return
	}

	if cancelInfo.Reason == "" {
		cancelInfo.Reason = "Cancelled via API"
	}
	if cancelInfo.CancelledBy == "" {
		cancelInfo.CancelledBy = "API user"
	}

	err := h.TaskQueue.CancelTask(taskID, cancelInfo.CancelledBy, cancelInfo.Reason)
	if err != nil {
		h.Logger.Warnf("Failed to cancel task %s: %v", taskID, err)
		http.Error(w, "Failed to cancel task: "+err.Error(), http.StatusBadRequest)
		return
	}

	h.Logger.Infof("Task cancelled: ID=%s, Reason=%s, By=%s",
		taskID, cancelInfo.Reason, cancelInfo.CancelledBy)

	w.WriteHeader(http.StatusOK)
	response := map[string]string{"status": "Task cancelled successfully"}
	json.NewEncoder(w).Encode(response)
}

func (h *TaskHandler) SearchTasks(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	page := 1
	pageSize := 10

	if p := query.Get("page"); p != "" {
		if val, err := strconv.Atoi(p); err == nil && val > 0 {
			page = val
		}
	}

	if ps := query.Get("pageSize"); ps != "" {
		if val, err := strconv.Atoi(ps); err == nil && val > 0 {
			pageSize = val
		}
	}

	filters := make(map[string]interface{})

	if status := query.Get("status"); status != "" {
		filters["status"] = status
	}

	if id := query.Get("id"); id != "" {
		filters["id"] = id
	}

	if tag := query.Get("tag"); tag != "" {
		filters["tag"] = tag
	}

	if createdAfter := query.Get("createdAfter"); createdAfter != "" {
		if t, err := time.Parse(time.RFC3339, createdAfter); err == nil {
			filters["createdAfter"] = t
		}
	}

	if createdBefore := query.Get("createdBefore"); createdBefore != "" {
		if t, err := time.Parse(time.RFC3339, createdBefore); err == nil {
			filters["createdBefore"] = t
		}
	}

	if priority := query.Get("priority"); priority != "" {
		if p, err := strconv.Atoi(priority); err == nil {
			filters["priority"] = queue.TaskPriority(p)
		}
	}

	tasks, total, err := h.TaskQueue.SearchTasks(filters, page, pageSize)
	if err != nil {
		h.Logger.Errorf("Search error: %v", err)
		http.Error(w, "Failed to search tasks: "+err.Error(), http.StatusInternalServerError)
		return
	}

	response := struct {
		Tasks      []*queue.Task `json:"tasks"`
		Page       int           `json:"page"`
		PageSize   int           `json:"pageSize"`
		TotalItems int           `json:"totalItems"`
		TotalPages int           `json:"totalPages"`
	}{
		Tasks:      tasks,
		Page:       page,
		PageSize:   pageSize,
		TotalItems: total,
		TotalPages: (total + pageSize - 1) / pageSize,
	}

	h.Logger.Infof("Task search: found %d tasks (page %d/%d)",
		len(tasks), page, response.TotalPages)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (h *TaskHandler) RegisterWebhook(w http.ResponseWriter, r *http.Request) {
	var wh webhook.Webhook
	if err := json.NewDecoder(r.Body).Decode(&wh); err != nil {
		h.Logger.Warnf("Invalid webhook data: %v", err)
		http.Error(w, "Invalid webhook data", http.StatusBadRequest)
		return
	}

	if wh.ID == "" {
		wh.ID = uuid.New().String()
	}

	if err := h.WebhookManager.RegisterWebhook(&wh); err != nil {
		h.Logger.Warnf("Failed to register webhook: %v", err)
		http.Error(w, "Failed to register webhook: "+err.Error(), http.StatusBadRequest)
		return
	}

	h.Logger.Infof("Webhook registered: ID=%s, URL=%s", wh.ID, wh.URL)
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(wh)
}

func (h *TaskHandler) UnregisterWebhook(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	webhookID := vars["id"]

	if err := h.WebhookManager.UnregisterWebhook(webhookID); err != nil {
		h.Logger.Warnf("Failed to unregister webhook: %v", err)
		http.Error(w, "Failed to unregister webhook: "+err.Error(), http.StatusNotFound)
		return
	}

	h.Logger.Infof("Webhook unregistered: ID=%s", webhookID)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "Webhook unregistered"})
}

func (h *TaskHandler) ListWebhooks(w http.ResponseWriter, r *http.Request) {
	webhooks := h.WebhookManager.GetWebhooks()

	h.Logger.Infof("Webhooks listed: count=%d", len(webhooks))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(webhooks)
}

func (h *TaskHandler) GetWorkerPoolStatus(w http.ResponseWriter, r *http.Request) {
	if h.WorkerPool == nil {
		http.Error(w, "Worker pool not configured", http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"workerCount":        h.WorkerPool.GetWorkerCount(),
		"minWorkers":         h.WorkerPool.GetMinWorkers(),
		"maxWorkers":         h.WorkerPool.GetMaxWorkers(),
		"autoScalingEnabled": h.WorkerPool.IsAutoScalingEnabled(),
		"scaleUpThreshold":   h.WorkerPool.GetScaleUpThreshold(),
		"scaleDownThreshold": h.WorkerPool.GetScaleDownThreshold(),
		"workers":            h.WorkerPool.GetWorkerStatus(),
	}

	h.Logger.Info("Worker pool status requested")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (h *TaskHandler) UpdateWorkerPool(w http.ResponseWriter, r *http.Request) {
	if h.WorkerPool == nil {
		http.Error(w, "Worker pool not configured", http.StatusInternalServerError)
		return
	}

	var config struct {
		MinWorkers           *int     `json:"minWorkers,omitempty"`
		MaxWorkers           *int     `json:"maxWorkers,omitempty"`
		AutoScaling          *bool    `json:"autoScaling,omitempty"`
		ScaleUpThreshold     *float64 `json:"scaleUpThreshold,omitempty"`
		ScaleDownThreshold   *float64 `json:"scaleDownThreshold,omitempty"`
		DistributionStrategy *string  `json:"distributionStrategy,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
		h.Logger.Warnf("Invalid worker pool configuration: %v", err)
		http.Error(w, "Invalid configuration data", http.StatusBadRequest)
		return
	}

	if config.MinWorkers != nil && config.MaxWorkers != nil {
		h.WorkerPool.SetWorkerLimits(*config.MinWorkers, *config.MaxWorkers)
	} else if config.MinWorkers != nil {
		h.WorkerPool.SetMinWorkers(*config.MinWorkers)
	} else if config.MaxWorkers != nil {
		h.WorkerPool.SetMaxWorkers(*config.MaxWorkers)
	}

	if config.AutoScaling != nil {
		h.WorkerPool.SetAutoScaling(*config.AutoScaling)
	}

	if config.ScaleUpThreshold != nil && config.ScaleDownThreshold != nil {
		h.WorkerPool.SetScaleThresholds(*config.ScaleUpThreshold, *config.ScaleDownThreshold)
	}

	if config.DistributionStrategy != nil {
		if h.TaskDistributor != nil {
			h.TaskDistributor.SetDistributionStrategy(worker.DistributionStrategy(*config.DistributionStrategy))
		}
	}

	h.Logger.Info("Worker pool configuration updated")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "Worker pool configuration updated"})
}

func (h *TaskHandler) AdjustWorkerCount(w http.ResponseWriter, r *http.Request) {
	if h.WorkerPool == nil {
		http.Error(w, "Worker pool not configured", http.StatusInternalServerError)
		return
	}

	vars := mux.Vars(r)
	action := vars["action"]

	countStr := r.URL.Query().Get("count")
	count := 1 // Default is 1 worker

	if countStr != "" {
		if c, err := strconv.Atoi(countStr); err == nil && c > 0 {
			count = c
		}
	}

	var response map[string]string

	switch action {
	case "add":
		h.WorkerPool.AddWorkers(count)
		response = map[string]string{"status": "Workers added"}
	case "remove":
		h.WorkerPool.RemoveWorkers(count)
		response = map[string]string{"status": "Workers removed"}
	default:
		http.Error(w, "Invalid action, use 'add' or 'remove'", http.StatusBadRequest)
		return
	}

	h.Logger.Infof("Worker count adjusted: %s %d", action, count)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}
