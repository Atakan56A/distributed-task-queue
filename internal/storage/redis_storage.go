package storage

import (
	"context"
	"distributed-task-queue/internal/queue"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	taskPrefix        = "task:"
	taskSetKey        = "tasks"
	taskStatusPrefix  = "status:"
	nodePrefix        = "node:"
	nodeSetKey        = "nodes"
	leaderKey         = "leader"
	lockPrefix        = "lock:"
	taskLockPrefix    = "task:lock:"
	taskProcessingSet = "tasks:processing"
	taskCompletedSet  = "tasks:completed"
)

type RedisStorage struct {
	client *redis.Client
	ctx    context.Context
	nodeID string
}

func NewRedisStorage(redisAddr, password string, nodeID string) (*RedisStorage, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: password,
		DB:       0,
	})

	ctx := context.Background()

	if _, err := client.Ping(ctx).Result(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	storage := &RedisStorage{
		client: client,
		ctx:    ctx,
		nodeID: nodeID,
	}

	return storage, nil
}

func (r *RedisStorage) SaveTask(task *queue.Task) error {
	taskJSON, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("error marshaling task: %w", err)
	}

	taskKey := fmt.Sprintf("%s%s", taskPrefix, task.ID)
	statusKey := fmt.Sprintf("%s%s", taskStatusPrefix, string(task.Status))

	pipe := r.client.Pipeline()

	pipe.Set(r.ctx, taskKey, taskJSON, 0)

	pipe.SAdd(r.ctx, taskSetKey, task.ID)

	pipe.SAdd(r.ctx, statusKey, task.ID)

	for _, event := range task.History {
		if event.Status != task.Status {
			prevStatusKey := fmt.Sprintf("%s%s", taskStatusPrefix, string(event.Status))
			pipe.SRem(r.ctx, prevStatusKey, task.ID)
		}
	}

	_, err = pipe.Exec(r.ctx)
	if err != nil {
		return fmt.Errorf("error saving task to Redis: %w", err)
	}

	return nil
}

func (r *RedisStorage) GetTask(taskID string) (*queue.Task, error) {
	taskKey := fmt.Sprintf("%s%s", taskPrefix, taskID)

	taskJSON, err := r.client.Get(r.ctx, taskKey).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("task %s not found", taskID)
		}
		return nil, fmt.Errorf("error retrieving task from Redis: %w", err)
	}

	var task queue.Task
	if err := json.Unmarshal([]byte(taskJSON), &task); err != nil {
		return nil, fmt.Errorf("error unmarshaling task: %w", err)
	}

	return &task, nil
}

func (r *RedisStorage) GetTasksByStatus(status string) ([]*queue.Task, error) {
	statusKey := fmt.Sprintf("%s%s", taskStatusPrefix, status)

	taskIDs, err := r.client.SMembers(r.ctx, statusKey).Result()
	if err != nil {
		return nil, fmt.Errorf("error retrieving task IDs by status: %w", err)
	}

	var tasks []*queue.Task
	for _, id := range taskIDs {
		task, err := r.GetTask(id)
		if err != nil {

			fmt.Printf("Error retrieving task %s: %v\n", id, err)
			continue
		}
		tasks = append(tasks, task)
	}

	return tasks, nil
}

func (r *RedisStorage) GetAllTasks() ([]*queue.Task, error) {
	taskIDs, err := r.client.SMembers(r.ctx, taskSetKey).Result()
	if err != nil {
		return nil, fmt.Errorf("error retrieving all task IDs: %w", err)
	}

	var tasks []*queue.Task
	for _, id := range taskIDs {
		task, err := r.GetTask(id)
		if err != nil {

			fmt.Printf("Error retrieving task %s: %v\n", id, err)
			continue
		}
		tasks = append(tasks, task)
	}

	return tasks, nil
}

func (r *RedisStorage) Close() error {
	return r.client.Close()
}

func (r *RedisStorage) AcquireTaskLock(taskID, nodeID string, timeout time.Duration) (bool, error) {
	lockKey := fmt.Sprintf("%s%s", taskLockPrefix, taskID)

	isCompleted, err := r.client.SIsMember(r.ctx, taskCompletedSet, taskID).Result()
	if err != nil {
		return false, err
	}

	if isCompleted {
		return false, nil // Task is already completed, ignore
	}

	return r.client.SetNX(r.ctx, lockKey, nodeID, timeout).Result()
}

func (r *RedisStorage) ReleaseTaskLock(taskID, nodeID string) error {
	lockKey := fmt.Sprintf("%s%s", taskLockPrefix, taskID)

	script := redis.NewScript(`
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("DEL", KEYS[1])
        else
            return 0
        end
    `)

	_, err := script.Run(r.ctx, r.client, []string{lockKey}, nodeID).Result()
	return err
}

func (r *RedisStorage) MarkTaskCompleted(taskID string) error {
	pipe := r.client.Pipeline()

	pipe.SRem(r.ctx, taskProcessingSet, taskID)
	pipe.SAdd(r.ctx, taskCompletedSet, taskID)

	_, err := pipe.Exec(r.ctx)
	return err
}

func (r *RedisStorage) IsTaskCompleted(taskID string) (bool, error) {
	return r.client.SIsMember(r.ctx, taskCompletedSet, taskID).Result()
}
