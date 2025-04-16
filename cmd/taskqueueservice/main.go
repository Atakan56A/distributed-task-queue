package main

import (
	"context"
	"distributed-task-queue/api"
	"distributed-task-queue/config"
	"distributed-task-queue/internal/metrics"
	"distributed-task-queue/internal/queue"
	"distributed-task-queue/internal/scheduler"
	"distributed-task-queue/internal/storage"
	"distributed-task-queue/internal/webhook"
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
		cfg = &config.Config{
			QueueSize:   100,
			LogLevel:    "info",
			RetryCount:  3,
			WorkerCount: 5,
			APIPort:     8080,
		}
	}

	log := logger.NewLogger(cfg.LogLevel)
	log.Info("Starting Task Queue Service")

	metricsCollector := metrics.NewMetrics()

	taskQueue := queue.NewQueue()

	dataDir := cfg.DataDir
	fileStorage, err := storage.NewFileStorage(dataDir)
	if err != nil {
		log.Fatalf("Failed to initialize storage: %v", err)
	}
	defer fileStorage.Close()

	taskQueue.SetStorage(fileStorage)

	if err := taskQueue.RestoreTasks(); err != nil {
		log.Warnf("Failed to restore tasks: %v", err)
	} else {
		log.Info("Successfully restored tasks from storage")
	}

	taskChannel := make(chan *queue.Task, cfg.QueueSize)

	webhookManager := webhook.NewWebhookManager(log)

	taskQueue.SetWebhookNotifier(webhookManager)

	workerPool := worker.NewDynamicWorkerPool(
		cfg.WorkerCount,
		cfg.MinWorkers,
		cfg.MaxWorkers,
		taskChannel,
		metricsCollector,
		taskQueue,
		webhookManager,
		log,
	)

	taskDistributor := worker.NewTaskDistributor(workerPool, taskChannel)

	workerTaskChannels := make(map[int]chan *queue.Task)
	for _, w := range workerPool.GetWorkers() { // DOĞRU - GetWorkers() bir Worker slice'ı döndürür
		workerTaskChannels[w.ID] = make(chan *queue.Task, 10)
	}

	go taskDistributor.DistributeTasks(workerTaskChannels)

	taskScheduler := scheduler.NewScheduler(taskQueue, taskChannel, log, cfg.SchedulerInterval)
	go taskScheduler.Start()

	taskHandler := api.NewTaskHandler(taskQueue, log, metricsCollector, webhookManager, workerPool, taskDistributor)
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
				task := taskQueue.Dequeue()
				if task != nil {
					taskChannel <- task
					log.Infof("Task dequeued: ID=%s, Status=%s", task.ID, task.Status)
				}
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(60 * time.Second)
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

	log.Info("Saving all tasks before shutdown...")
	if err := fileStorage.Close(); err != nil {
		log.Errorf("Error saving tasks: %v", err)
	}

	log.Info("Server stopped")
}
