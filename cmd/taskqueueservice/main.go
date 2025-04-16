package main

import (
	"context"
	"distributed-task-queue/api"
	"distributed-task-queue/config"
	"distributed-task-queue/internal/cluster"
	"distributed-task-queue/internal/metrics"
	"distributed-task-queue/internal/queue"
	"distributed-task-queue/internal/scheduler"
	"distributed-task-queue/internal/storage"
	"distributed-task-queue/internal/webhook"
	"distributed-task-queue/internal/worker"
	"distributed-task-queue/pkg/logger"
	"fmt"

	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gorilla/mux"
)

func main() {
	cfg, err := config.LoadConfig("config1.json")

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

	var redisClient *redis.Client
	var clusterNode *cluster.Node
	var leaderElector *cluster.LeaderElector
	var distributedScheduler *cluster.DistributedScheduler
	var healthMonitor *cluster.HealthMonitor
	var clusterHandler *api.ClusterHandler
	var storageImpl queue.Storage        // Burada bir storage değişkeni tanımlayalım
	var fileStorage *storage.FileStorage // Eğer file storage ise direct erişim için

	metricsCollector := metrics.NewMetrics()

	taskQueue := queue.NewQueue()

	taskChannel := make(chan *queue.Task, cfg.QueueSize)

	if cfg.ClusterEnabled {
		log.Info("Starting in cluster mode")

		redisClient = redis.NewClient(&redis.Options{
			Addr:         cfg.RedisAddress,
			Password:     cfg.RedisPassword,
			DB:           0,
			DialTimeout:  5 * time.Second,
			ReadTimeout:  2 * time.Second,
			WriteTimeout: 2 * time.Second,
			PoolSize:     10,
			MinIdleConns: 5,
			PoolTimeout:  1 * time.Minute,
		})

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if _, err := redisClient.Ping(ctx).Result(); err != nil {
			log.Fatalf("Failed to connect to Redis: %v", err)
		}

		redisStorage, err := storage.NewRedisStorage(cfg.RedisAddress, cfg.RedisPassword, "node-id")
		if err != nil {
			log.Fatalf("Failed to create Redis storage: %v", err)
		}

		storageImpl = redisStorage
		taskQueue.SetStorage(storageImpl)

		if err := taskQueue.RestoreTasks(); err != nil {
			log.Warnf("Failed to restore tasks: %v", err)
		} else {
			log.Info("Successfully restored tasks from Redis")
		}

		clusterNode = cluster.NewNode(cfg.NodeAddress, redisClient)
		if err := clusterNode.Start(); err != nil {
			log.Fatalf("Failed to start cluster node: %v", err)
		}

		leaderElector = cluster.NewLeaderElector(clusterNode, redisClient, log)
		leaderElector.Start()

		taskHandlerFn := func(task *queue.Task) {
			taskChannel <- task
		}

		distributedScheduler = cluster.NewDistributedScheduler(
			clusterNode,
			leaderElector,
			redisClient,
			taskQueue,
			log,
			taskHandlerFn,
		)
		distributedScheduler.Start()

		healthMonitor = cluster.NewHealthMonitor(
			clusterNode,
			leaderElector,
			redisClient,
			distributedScheduler,
			log,
		)
		healthMonitor.Start()

		clusterHandler = api.NewClusterHandler(
			clusterNode,
			leaderElector,
			healthMonitor,
			distributedScheduler,
			log,
		)
	} else {
		log.Info("Starting in standalone mode")

		dataDir := cfg.DataDir
		var err error
		fileStorage, err = storage.NewFileStorage(dataDir)
		if err != nil {
			log.Fatalf("Failed to initialize storage: %v", err)
		}

		storageImpl = fileStorage
		taskQueue.SetStorage(storageImpl)

		if err := taskQueue.RestoreTasks(); err != nil {
			log.Warnf("Failed to restore tasks: %v", err)
		} else {
			log.Info("Successfully restored tasks from storage")
		}

	}

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
	for _, w := range workerPool.GetWorkers() {
		workerTaskChannels[w.ID] = make(chan *queue.Task, 10)
	}

	go taskDistributor.DistributeTasks(workerTaskChannels)

	taskScheduler := scheduler.NewScheduler(taskQueue, taskChannel, log, cfg.SchedulerInterval)
	go taskScheduler.Start()

	taskHandler := api.NewTaskHandler(taskQueue, log, metricsCollector, webhookManager, workerPool, taskDistributor)

	var router *mux.Router
	if cfg.ClusterEnabled {
		router = api.NewRouter(taskHandler, clusterHandler)
	} else {
		router = api.NewRouter(taskHandler, nil)
	}

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

	if cfg.ClusterEnabled && redisClient != nil {

		processingTasks, err := redisClient.SMembers(context.Background(), "tasks:processing").Result()
		if err != nil {
			log.Errorf("Error checking processing tasks: %v", err)
		} else {
			log.Infof("Tasks in processing set: %d", len(processingTasks))
			for _, taskID := range processingTasks {
				log.Infof("  - Task %s in processing set", taskID)
			}
		}

		completedTasks, err := redisClient.SMembers(context.Background(), "tasks:completed").Result()
		if err != nil {
			log.Errorf("Error checking completed tasks: %v", err)
		} else {
			log.Infof("Tasks in completed set: %d", len(completedTasks))
		}

		if cfg.CleanRedisOnStart {
			log.Warn("Selectively cleaning Redis processing set")

			for _, taskID := range processingTasks {

				lockKey := fmt.Sprintf("task:lock:%s", taskID)

				lockExists, _ := redisClient.Exists(context.Background(), lockKey).Result()

				if lockExists == 0 {

					log.Warnf("Removing stale task %s from processing set (no lock found)", taskID)
					redisClient.SRem(context.Background(), "tasks:processing", taskID)
				} else {

					ttl, _ := redisClient.TTL(context.Background(), lockKey).Result()

					if ttl < 0 || ttl > 6*time.Hour {
						log.Warnf("Removing stale task %s from processing set (expired lock)", taskID)
						redisClient.SRem(context.Background(), "tasks:processing", taskID)
						redisClient.Del(context.Background(), lockKey)
					}
				}
			}
		}
	}

	go func() {
		ticker := time.NewTicker(100 * time.Millisecond) // 100ms aralıkla kontrol et
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:

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
	cancel() // Önce ana context'i iptal et

	log.Info("Waiting for goroutines to finish...")
	time.Sleep(500 * time.Millisecond)

	log.Info("Stopping worker pool...")
	workerPool.Stop()
	log.Info("Worker pool stopped")

	log.Info("Stopping task scheduler...")
	taskScheduler.Stop()
	log.Info("Task scheduler stopped")
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}
	log.Info("HTTP server stopped")

	if cfg.ClusterEnabled {
		log.Info("Shutting down cluster components...")

		log.Info("Stopping distributed scheduler...")
		distributedScheduler.Stop()
		log.Info("Distributed scheduler stopped")

		log.Info("Stopping health monitor...")
		healthMonitor.Stop()
		log.Info("Health monitor stopped")

		log.Info("Stopping leader elector...")
		leaderElector.Stop()
		log.Info("Leader elector stopped")

		log.Info("Stopping cluster node...")
		clusterNode.Stop()
		log.Info("Cluster node stopped")

		log.Info("Closing Redis connection...")
		redisClient.Close()
		log.Info("Redis connection closed")
	} else {
		log.Info("Shutting down standalone components...")

		log.Info("Saving all tasks before shutdown...")
		if fileStorage != nil {
			if err := fileStorage.Close(); err != nil {
				log.Errorf("Error saving tasks: %v", err)
			}
		}
	}

	log.Info("Server stopped")
}
