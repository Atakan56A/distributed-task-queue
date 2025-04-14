package api

import (
	"github.com/gorilla/mux"
)

func NewRouter(taskHandler *TaskHandler) *mux.Router {
	r := mux.NewRouter()

	r.HandleFunc("/tasks", taskHandler.ListTasks).Methods("GET")
	r.HandleFunc("/tasks", taskHandler.AddTask).Methods("POST")
	r.HandleFunc("/tasks/{id}", taskHandler.GetTaskStatus).Methods("GET")

	return r
}
