package main

import (
	"context"
	"distributed-task-queue/api"
	"distributed-task-queue/config"
	"distributed-task-queue/internal/metrics"
	"distributed-task-queue/internal/queue"
	"distributed-task-queue/internal/scheduler"
	"distributed-task-queue/internal/worker"
	"distributed-task-queue/pkg/logger"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

func main() {
	cfg, err := config.LoadConfig("config.json")
	if err != nil {
		log := logger.NewLogger("warn")
		log.Warnf("Error loading configuration: %v, using defaults", err)
		cfg = &config.Config{
			QueueSize:   100,
			LogLevel:    "info",
			RetryCount:  3,
			WorkerCount: 5,
			APIPort:     8080,
		}
	}

	log := logger.NewLogger(cfg.LogLevel)

	metricsCollector := metrics.NewMetrics()

	taskQueue := queue.NewQueue()

	taskChannel := make(chan *queue.Task, cfg.QueueSize)

	workerPool := worker.NewWorkerPool(cfg.WorkerCount, taskChannel, metricsCollector)

	taskScheduler := scheduler.NewScheduler(taskQueue, taskChannel, log)
	go taskScheduler.Start()

	taskHandler := api.NewTaskHandler(taskQueue, log, metricsCollector)
	router := api.NewRouter(taskHandler)

	port := strconv.Itoa(cfg.APIPort)
	server := &http.Server{
		Addr:    ":" + port,
		Handler: router,
	}

	go func() {
		log.Infof("HTTP server listening on port %s", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

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
					log.Infof("Task dequeued: ID=%s, Status=%s", task.ID, task.Status)
				}
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(60 * time.Second) // Her dakika
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				stats := metricsCollector.GetSummaryStats()
				log.Infof("System metrics: Tasks=%d, Success=%d, Failed=%d, AvgTime=%v",
					stats["totalTasks"], stats["successfulTasks"], stats["failedTasks"], stats["avgProcessingTime"])
			case <-ctx.Done():
				return
			}
		}
	}()

	log.Info("Task Queue Service started successfully")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("Shutting down server...")
	cancel()

	taskScheduler.Stop()

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	workerPool.Stop()

	log.Info("Server stopped")
}
