package api

import (
	"github.com/gorilla/mux"
)

func NewRouter(taskHandler *TaskHandler) *mux.Router {
	r := mux.NewRouter()

	r.HandleFunc("/tasks", taskHandler.ListTasks).Methods("GET")
	r.HandleFunc("/tasks", taskHandler.AddTask).Methods("POST")
	r.HandleFunc("/tasks/{id}", taskHandler.GetTaskStatus).Methods("GET")
	r.HandleFunc("/metrics", taskHandler.GetMetrics).Methods("GET")
	r.HandleFunc("/tasks/{id}/details", taskHandler.GetTaskDetails).Methods("GET")
	r.HandleFunc("/deadletter", taskHandler.ListDeadLetterTasks).Methods("GET")
	r.HandleFunc("/deadletter/{id}/retry", taskHandler.RetryDeadLetterTask).Methods("POST")
	r.HandleFunc("/tasks/{id}/cancel", taskHandler.CancelTask).Methods("POST")
	r.HandleFunc("/tasks/search", taskHandler.SearchTasks).Methods("GET")
	r.HandleFunc("/webhooks", taskHandler.ListWebhooks).Methods("GET")
	r.HandleFunc("/webhooks", taskHandler.RegisterWebhook).Methods("POST")
	r.HandleFunc("/webhooks/{id}", taskHandler.UnregisterWebhook).Methods("DELETE")
	r.HandleFunc("/workers", taskHandler.GetWorkerPoolStatus).Methods("GET")
	r.HandleFunc("/workers", taskHandler.UpdateWorkerPool).Methods("PUT")
	r.HandleFunc("/workers/{action}", taskHandler.AdjustWorkerCount).Methods("POST")

	return r
}
