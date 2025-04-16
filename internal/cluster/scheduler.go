package cluster

import (
	"context"
	"distributed-task-queue/internal/queue"
	"distributed-task-queue/pkg/logger"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	taskAssignmentChannel = "task:assignments"
	nodeTaskPrefix        = "node:tasks:"
	schedulerInterval     = 2 * time.Second
	taskProcessingSet     = "tasks:processing"
	taskCompletedSet      = "tasks:completed"
	taskLockPrefix        = "task:lock:"
)

type DistributedScheduler struct {
	node          *Node
	leaderElector *LeaderElector
	redisClient   *redis.Client
	taskQueue     *queue.Queue
	logger        *logger.Logger
	ctx           context.Context
	stopChan      chan struct{}
	mu            sync.RWMutex
	isActive      bool
	pubSub        *redis.PubSub
	taskHandlerFn func(*queue.Task)
}

type TaskAssignment struct {
	TaskID     string    `json:"taskId"`
	NodeID     string    `json:"nodeId"`
	AssignedAt time.Time `json:"assignedAt"`
}

func NewDistributedScheduler(
	node *Node,
	leaderElector *LeaderElector,
	redisClient *redis.Client,
	taskQueue *queue.Queue,
	logger *logger.Logger,
	taskHandlerFn func(*queue.Task),
) *DistributedScheduler {

	ctx := context.Background()

	scheduler := &DistributedScheduler{
		node:          node,
		leaderElector: leaderElector,
		redisClient:   redisClient,
		taskQueue:     taskQueue,
		logger:        logger,
		ctx:           ctx,
		stopChan:      make(chan struct{}),
		isActive:      false,
		taskHandlerFn: taskHandlerFn,
	}

	leaderElector.SetCallbacks(
		scheduler.onBecomeLeader,   // Called when node becomes leader
		scheduler.onLoseLeadership, // Called when node loses leadership
	)

	return scheduler
}

func (ds *DistributedScheduler) Start() {

	ds.subscribeToTaskAssignments()

	if ds.leaderElector.IsLeader() {
		ds.onBecomeLeader()
	}
}

func (ds *DistributedScheduler) Stop() {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if ds.isActive {
		ds.logger.Info("Stopping distributed scheduler...")
		ds.isActive = false

		close(ds.stopChan)

		time.Sleep(200 * time.Millisecond)

		if ds.pubSub != nil {
			ds.logger.Info("Closing PubSub connection...")
			if err := ds.pubSub.Close(); err != nil {
				ds.logger.Errorf("Error closing PubSub: %v", err)
			}
		}

		ds.logger.Info("Distributed scheduler stopped successfully")
	}
}

func (ds *DistributedScheduler) onBecomeLeader() {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if !ds.isActive {
		ds.isActive = true
		ds.logger.Info("Starting distributed task scheduling")
		go ds.scheduleLoop()
	}
}

func (ds *DistributedScheduler) onLoseLeadership() {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if ds.isActive {
		ds.isActive = false
		ds.logger.Info("Stopping distributed task scheduling")
	}
}

func (ds *DistributedScheduler) scheduleLoop() {
	ticker := time.NewTicker(schedulerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:

			if ds.leaderElector.IsLeader() {
				if err := ds.distributeTasks(); err != nil {
					ds.logger.Errorf("Error distributing tasks: %v", err)
				}
			}

		case <-ds.stopChan:
			return
		}
	}
}

func (ds *DistributedScheduler) distributeTasks() error {
	ds.ensureRedisConnection()

	nodes, err := ds.node.GetAllNodes()
	if err != nil {
		return fmt.Errorf("failed to get available nodes: %w", err)
	}

	var availableNodes []*NodeInfo
	for _, node := range nodes {
		if node.Status == NodeStatusUp {
			availableNodes = append(availableNodes, node)
		}
	}

	if len(availableNodes) == 0 {
		return fmt.Errorf("no available nodes for task distribution")
	}

	pendingTasks := ds.taskQueue.GetPendingTasks()

	ds.logger.Infof("Found %d pending tasks to process", len(pendingTasks))
	for i, task := range pendingTasks {
		ds.logger.Debugf("Pending task %d: ID=%s, Status=%s, Created=%s",
			i, task.ID, task.Status, task.CreatedAt.Format(time.RFC3339))
	}

	var tasksToDistribute []*queue.Task
	var skippedCompletedTasks, skippedProcessingTasks, skippedOtherReasons int

	for _, task := range pendingTasks {

		if task.Status == queue.TaskStatusCompleted {
			ds.logger.Debugf("Task %s is already completed in internal state, skipping", task.ID)
			skippedCompletedTasks++
			continue
		}

		isCompleted, err := ds.taskQueue.GetStorage().IsTaskCompleted(task.ID)
		if err != nil {
			ds.logger.Errorf("Error checking task completion: %v", err)
			skippedOtherReasons++
			continue
		}

		if isCompleted {
			ds.logger.Debugf("Task %s is marked as completed in Redis, skipping", task.ID)
			skippedCompletedTasks++
			continue
		}

		isProcessing, err := ds.redisClient.SIsMember(ds.ctx, taskProcessingSet, task.ID).Result()
		if err != nil {
			ds.logger.Errorf("Error checking if task is processing: %v", err)
			skippedOtherReasons++
			continue
		}

		if isProcessing {
			ds.logger.Debugf("Task %s is already being processed in Redis, skipping", task.ID)
			skippedProcessingTasks++
			continue
		}

		tasksToDistribute = append(tasksToDistribute, task)
	}

	if len(pendingTasks) > 0 {
		ds.logger.Infof("Distribution summary: %d pending tasks, %d to distribute (%d completed, %d processing, %d other reasons)",
			len(pendingTasks), len(tasksToDistribute), skippedCompletedTasks, skippedProcessingTasks, skippedOtherReasons)
	}

	for i, task := range tasksToDistribute {
		nodeIndex := i % len(availableNodes)
		targetNode := availableNodes[nodeIndex]

		assignment := TaskAssignment{
			TaskID:     task.ID,
			NodeID:     targetNode.ID,
			AssignedAt: time.Now(),
		}

		assigned, err := ds.assignTaskToNode(assignment)
		if err != nil {
			ds.logger.Errorf("Error assigning task %s to node %s: %v",
				task.ID, targetNode.ID, err)
			continue
		}

		if assigned {
			ds.logger.Infof("Task %s assigned to node %s", task.ID, targetNode.ID)
		}
	}

	return nil
}

func (ds *DistributedScheduler) ensureRedisConnection() {
	if ds.redisClient == nil {
		return
	}

	_, err := ds.redisClient.Ping(ds.ctx).Result()
	if err != nil {
		ds.logger.Warnf("Redis connection check failed: %v, attempting to reconnect", err)

		for i := 0; i < 3; i++ {

			if ds.pubSub != nil {
				ds.pubSub.Close()
			}

			newClient := redis.NewClient(&redis.Options{
				Addr:         ds.redisClient.Options().Addr,
				Password:     ds.redisClient.Options().Password,
				DB:           ds.redisClient.Options().DB,
				DialTimeout:  5 * time.Second,
				ReadTimeout:  2 * time.Second,
				WriteTimeout: 2 * time.Second,
			})

			if _, err := newClient.Ping(ds.ctx).Result(); err == nil {
				ds.logger.Info("Successfully reconnected to Redis")

				ds.redisClient = newClient
				ds.pubSub = ds.redisClient.Subscribe(ds.ctx, taskAssignmentChannel)
				return
			}

			ds.logger.Warnf("Reconnect attempt %d failed: %v", i+1, err)
			time.Sleep(1 * time.Second)
		}

		ds.logger.Error("Failed to reconnect to Redis after multiple attempts")
	}
}

func (ds *DistributedScheduler) assignTaskToNode(assignment TaskAssignment) (bool, error) {
	storage := ds.taskQueue.GetStorage()

	isCompleted, err := storage.IsTaskCompleted(assignment.TaskID)
	if err != nil {
		return false, fmt.Errorf("error checking task completion: %w", err)
	}

	if isCompleted {
		ds.logger.Infof("Task %s is already completed, skipping assignment", assignment.TaskID)
		return false, nil // Görev atanmadı, hata yok
	}

	isProcessing, err := ds.redisClient.SIsMember(ds.ctx, taskProcessingSet, assignment.TaskID).Result()
	if err != nil {
		return false, fmt.Errorf("error checking if task is processing: %w", err)
	}

	if isProcessing {
		ds.logger.Infof("Task %s is already being processed, skipping assignment", assignment.TaskID)
		return false, nil // Görev atanmadı, hata yok
	}

	pipe := ds.redisClient.Pipeline()
	nodeTasksKey := fmt.Sprintf("%s%s", nodeTaskPrefix, assignment.NodeID)

	pipe.SAdd(ds.ctx, taskProcessingSet, assignment.TaskID)
	pipe.SAdd(ds.ctx, nodeTasksKey, assignment.TaskID)

	assignmentJSON, _ := json.Marshal(assignment)
	pipe.Publish(ds.ctx, taskAssignmentChannel, assignmentJSON)

	_, err = pipe.Exec(ds.ctx)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (ds *DistributedScheduler) subscribeToTaskAssignments() {
	ds.pubSub = ds.redisClient.Subscribe(ds.ctx, taskAssignmentChannel)

	go func() {

		ctx, cancel := context.WithCancel(ds.ctx)
		defer cancel()

		stopChan := ds.stopChan

		go func() {
			<-stopChan
			ds.logger.Info("Stopping PubSub listener...")
			cancel() // Yerel context'i iptal et
		}()

		for {
			select {
			case <-ctx.Done():
				ds.logger.Info("PubSub context cancelled, exiting")
				return
			default:

				msg, err := ds.pubSub.ReceiveTimeout(ctx, 500*time.Millisecond)
				if err != nil {
					if err == redis.Nil || ctx.Err() != nil {

						continue
					} else if strings.Contains(err.Error(), "i/o timeout") {

						ds.logger.Debugf("Redis timeout (normal): %v", err)

						if rand.Intn(10) == 0 {
							ds.ensureRedisConnection()
						}
						continue
					} else if strings.Contains(err.Error(), "connection closed") ||
						strings.Contains(err.Error(), "use of closed network connection") {
						ds.logger.Warn("Redis connection closed, attempting to reconnect")

						ds.ensureRedisConnection()
						time.Sleep(500 * time.Millisecond)
						continue
					}

					ds.logger.Errorf("Error receiving task assignment: %v", err)
					time.Sleep(500 * time.Millisecond)
					continue
				}

				switch m := msg.(type) {
				case *redis.Message:
					var assignment TaskAssignment
					if err := json.Unmarshal([]byte(m.Payload), &assignment); err != nil {
						ds.logger.Errorf("Error unmarshaling task assignment: %v", err)
						continue
					}

					if assignment.NodeID == ds.node.Info.ID {
						ds.logger.Infof("Received task assignment: %s", assignment.TaskID)

						task, err := ds.taskQueue.GetTaskDetails(assignment.TaskID)
						if err != nil {
							ds.logger.Errorf("Error fetching assigned task %s: %v",
								assignment.TaskID, err)
							continue
						}

						if ds.taskHandlerFn != nil {
							go ds.taskHandlerFn(task)
						}
					}
				case *redis.Subscription:
					ds.logger.Infof("Subscription changed: %s %s", m.Kind, m.Channel)
				default:
					ds.logger.Warnf("Received unknown message type: %T", msg)
				}
			}
		}
	}()
}

func (ds *DistributedScheduler) GetNodeTasks(nodeID string) ([]string, error) {
	nodeTasksKey := fmt.Sprintf("%s%s", nodeTaskPrefix, nodeID)
	return ds.redisClient.SMembers(ds.ctx, nodeTasksKey).Result()
}

func (ds *DistributedScheduler) ReassignNodeTasks(fromNodeID, toNodeID string) error {

	if !ds.leaderElector.IsLeader() {
		return fmt.Errorf("only the leader node can reassign tasks")
	}

	tasks, err := ds.GetNodeTasks(fromNodeID)
	if err != nil {
		return fmt.Errorf("error getting tasks for node %s: %w", fromNodeID, err)
	}

	ds.logger.Infof("Reassigning %d tasks from node %s to node %s",
		len(tasks), fromNodeID, toNodeID)

	for _, taskID := range tasks {

		assignment := TaskAssignment{
			TaskID:     taskID,
			NodeID:     toNodeID,
			AssignedAt: time.Now(),
		}

		nodeTasksKeyOld := fmt.Sprintf("%s%s", nodeTaskPrefix, fromNodeID)
		nodeTasksKeyNew := fmt.Sprintf("%s%s", nodeTaskPrefix, toNodeID)

		pipe := ds.redisClient.Pipeline()
		pipe.SRem(ds.ctx, nodeTasksKeyOld, taskID)
		pipe.SAdd(ds.ctx, nodeTasksKeyNew, taskID)

		assignmentJSON, _ := json.Marshal(assignment)
		pipe.Publish(ds.ctx, taskAssignmentChannel, assignmentJSON)

		if _, err := pipe.Exec(ds.ctx); err != nil {
			ds.logger.Errorf("Error reassigning task %s: %v", taskID, err)
		}
	}

	return nil
}
