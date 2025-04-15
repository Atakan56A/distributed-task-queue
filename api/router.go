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

	return r
}
