package cluster

import (
	"context"
	"distributed-task-queue/pkg/logger"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	leaderKey              = "cluster:leader"
	leaderLockKey          = "cluster:leader:lock"
	leaderLockDuration     = 15 * time.Second
	leaderElectionInterval = 5 * time.Second
)

type LeaderElector struct {
	node        *Node
	redisClient *redis.Client
	ctx         context.Context
	logger      *logger.Logger
	stopChan    chan struct{}

	onBecomeLeader   func()
	onLoseLeadership func()
}

func NewLeaderElector(node *Node, redisClient *redis.Client, logger *logger.Logger) *LeaderElector {
	return &LeaderElector{
		node:        node,
		redisClient: redisClient,
		ctx:         context.Background(),
		logger:      logger,
		stopChan:    make(chan struct{}),
	}
}

func (le *LeaderElector) SetCallbacks(onBecomeLeader, onLoseLeadership func()) {
	le.onBecomeLeader = onBecomeLeader
	le.onLoseLeadership = onLoseLeadership
}

func (le *LeaderElector) Start() {
	go le.leaderElectionLoop()
}

func (le *LeaderElector) Stop() {
	close(le.stopChan)
}

func (le *LeaderElector) leaderElectionLoop() {
	ticker := time.NewTicker(leaderElectionInterval)
	defer ticker.Stop()

	wasLeader := false

	for {
		select {
		case <-ticker.C:
			isLeader := le.tryToBecomeLeader()

			if isLeader != wasLeader {
				if isLeader {
					le.logger.Info("This node has become the cluster leader")
					le.node.Info.IsLeader = true
					if le.onBecomeLeader != nil {
						le.onBecomeLeader()
					}
				} else {
					le.logger.Info("This node is no longer the cluster leader")
					le.node.Info.IsLeader = false
					if le.onLoseLeadership != nil {
						le.onLoseLeadership()
					}
				}
				wasLeader = isLeader

				if err := le.node.updateInfo(); err != nil {
					le.logger.Errorf("Failed to update node info: %v", err)
				}
			}

		case <-le.stopChan:

			if wasLeader {
				le.relinquishLeadership()
			}
			return
		}
	}
}

func (le *LeaderElector) tryToBecomeLeader() bool {

	success, err := le.redisClient.SetNX(
		le.ctx,
		leaderKey,
		le.node.Info.ID,
		leaderLockDuration,
	).Result()

	if err != nil {
		le.logger.Errorf("Error in leader election: %v", err)
		return false
	}

	if success {
		return true
	}

	currentLeader, err := le.redisClient.Get(le.ctx, leaderKey).Result()
	if err != nil {
		if err != redis.Nil {
			le.logger.Errorf("Error checking current leader: %v", err)
		}
		return false
	}

	if currentLeader == le.node.Info.ID {

		err := le.redisClient.Set(le.ctx, leaderKey, le.node.Info.ID, leaderLockDuration).Err()
		if err != nil {
			le.logger.Errorf("Error refreshing leader lock: %v", err)
			return false
		}
		return true
	}

	return false
}

func (le *LeaderElector) relinquishLeadership() {

	le.redisClient.Del(le.ctx, leaderKey)
	le.node.Info.IsLeader = false
	le.node.updateInfo()
	le.logger.Info("Leadership relinquished")
}

func (le *LeaderElector) GetCurrentLeader() (string, error) {
	leaderID, err := le.redisClient.Get(le.ctx, leaderKey).Result()
	if err != nil {
		if err == redis.Nil {
			return "", fmt.Errorf("no leader currently elected")
		}
		return "", fmt.Errorf("error getting current leader: %w", err)
	}

	return leaderID, nil
}

func (le *LeaderElector) IsLeader() bool {
	return le.node.Info.IsLeader
}
