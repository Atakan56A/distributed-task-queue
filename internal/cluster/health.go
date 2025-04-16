package cluster

import (
	"context"
	"distributed-task-queue/pkg/logger"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	nodeHealthCheckInterval = 15 * time.Second
	nodeFailureThreshold    = 30 * time.Second
)

type HealthMonitor struct {
	node          *Node
	leaderElector *LeaderElector
	redisClient   *redis.Client
	scheduler     *DistributedScheduler
	logger        *logger.Logger
	ctx           context.Context
	stopChan      chan struct{}
	checkInterval time.Duration
}

func NewHealthMonitor(
	node *Node,
	leaderElector *LeaderElector,
	redisClient *redis.Client,
	scheduler *DistributedScheduler,
	logger *logger.Logger,
) *HealthMonitor {

	return &HealthMonitor{
		node:          node,
		leaderElector: leaderElector,
		redisClient:   redisClient,
		scheduler:     scheduler,
		logger:        logger,
		ctx:           context.Background(),
		stopChan:      make(chan struct{}),
		checkInterval: nodeHealthCheckInterval,
	}
}

func (hm *HealthMonitor) Start() {
	go hm.healthCheckLoop()
}

func (hm *HealthMonitor) Stop() {
	close(hm.stopChan)
}

func (hm *HealthMonitor) healthCheckLoop() {
	ticker := time.NewTicker(hm.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:

			if hm.leaderElector.IsLeader() {
				if err := hm.performHealthCheck(); err != nil {
					hm.logger.Errorf("Error performing health check: %v", err)
				}
			}

		case <-hm.stopChan:
			return
		}
	}
}

func (hm *HealthMonitor) performHealthCheck() error {

	nodes, err := hm.node.GetAllNodes()
	if err != nil {
		return fmt.Errorf("failed to get nodes: %w", err)
	}

	var failedNodes []*NodeInfo

	for _, nodeInfo := range nodes {

		if nodeInfo.ID == hm.node.Info.ID {
			continue
		}

		if time.Since(nodeInfo.LastHeartbeat) > nodeFailureThreshold {
			failedNodes = append(failedNodes, nodeInfo)

			nodeInfo.Status = NodeStatusDown

			nodeKey := fmt.Sprintf("node:%s", nodeInfo.ID)
			_, err := hm.redisClient.Get(hm.ctx, nodeKey).Result()

			if err != nil && err != redis.Nil {
				hm.logger.Errorf("Error checking failed node %s: %v", nodeInfo.ID, err)
				continue
			}

			if err != redis.Nil {
				nodeInfo.Status = NodeStatusDown

				if nodeJSON, err := context.WithTimeout(hm.ctx, 5*time.Second); err == nil {
					hm.redisClient.Set(hm.ctx, nodeKey, nodeJSON, 0)
				}
			}
		}
	}

	if len(failedNodes) > 0 {
		hm.handleFailedNodes(failedNodes)
	}

	return nil
}

func (hm *HealthMonitor) handleFailedNodes(failedNodes []*NodeInfo) {

	allNodes, err := hm.node.GetAllNodes()
	if err != nil {
		hm.logger.Errorf("Failed to get node list for recovery: %v", err)
		return
	}

	var healthyNodes []*NodeInfo
	for _, node := range allNodes {
		if node.Status == NodeStatusUp && node.ID != hm.node.Info.ID {
			healthyNodes = append(healthyNodes, node)
		}
	}

	healthyNodes = append(healthyNodes, hm.node.Info)

	for _, failedNode := range failedNodes {
		hm.logger.Warnf("Node failure detected: %s (Last heartbeat: %v)",
			failedNode.ID, failedNode.LastHeartbeat)

		if len(healthyNodes) == 0 {
			hm.logger.Warn("No other healthy nodes available, reassigning tasks to self")
			err := hm.scheduler.ReassignNodeTasks(failedNode.ID, hm.node.Info.ID)
			if err != nil {
				hm.logger.Errorf("Error reassigning tasks from %s: %v", failedNode.ID, err)
			}
			continue
		}

		for i, healthyNode := range healthyNodes {

			if i == 0 {
				err := hm.scheduler.ReassignNodeTasks(failedNode.ID, healthyNode.ID)
				if err != nil {
					hm.logger.Errorf("Error reassigning tasks from %s to %s: %v",
						failedNode.ID, healthyNode.ID, err)
				} else {
					hm.logger.Infof("Successfully reassigned tasks from node %s to %s",
						failedNode.ID, healthyNode.ID)
				}
			}
		}
	}
}
