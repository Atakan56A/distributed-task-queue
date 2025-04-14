package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"distributed-task-queue/api"
	"distributed-task-queue/config"
	"distributed-task-queue/internal/queue"
	"distributed-task-queue/internal/worker"
)

func main() {
	// Load configuration
	cfg, err := config.LoadConfig("config.json")
	if err != nil {
		log.Printf("Error loading configuration: %v, using defaults", err)
		cfg = &config.Config{
			QueueSize:   100,
			LogLevel:    "info",
			RetryCount:  3,
			WorkerCount: 5,
			APIPort:     8080,
		}
	}

	// Initialize the task queue
	taskQueue := queue.NewQueue()

	// Create a channel for workers
	taskChannel := make(chan *queue.Task, cfg.QueueSize)

	// Start worker pool
	workerPool := worker.NewWorkerPool(cfg.WorkerCount, taskChannel)

	// Setup API handlers
	taskHandler := api.NewTaskHandler(taskQueue)
	router := api.NewRouter(taskHandler)

	// Start HTTP server
	port := strconv.Itoa(cfg.APIPort)
	server := &http.Server{
		Addr:    ":" + port,
		Handler: router,
	}

	// Start server in a goroutine
	go func() {
		log.Printf("HTTP server listening on port %s", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// Start handling tasks in a separate goroutine
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if task := taskQueue.Dequeue(); task != nil {
					taskChannel <- task
				} else {
					// Kısa bir süre bekleyelim CPU kullanımını azaltmak için
					time.Sleep(100 * time.Millisecond)
				}
			}
		}
	}()

	log.Println("Task Queue Service started successfully")

	// Graceful shutdown için sinyal bekle
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")
	cancel() // Worker goroutine'leri iptal et

	// Sunucuyu kapat
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	// Worker pool'u durdur
	workerPool.Stop()

	log.Println("Server stopped")
}
