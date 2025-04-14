package api

import (
	"distributed-task-queue/internal/queue"
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

type TaskHandler struct {
	TaskQueue *queue.Queue
}

func NewTaskHandler(taskQueue *queue.Queue) *TaskHandler {
	return &TaskHandler{TaskQueue: taskQueue}
}

func (h *TaskHandler) AddTask(w http.ResponseWriter, r *http.Request) {
	var task queue.Task
	if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := h.TaskQueue.AddTask(task); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func (h *TaskHandler) GetTaskStatus(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	taskID := vars["id"]

	status, err := h.TaskQueue.GetTaskStatus(taskID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	response := map[string]string{"id": taskID, "status": status}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (h *TaskHandler) ListTasks(w http.ResponseWriter, r *http.Request) {

	statusFilter := r.URL.Query().Get("status")

	tasks, err := h.TaskQueue.ListTasks(statusFilter)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tasks)
}
