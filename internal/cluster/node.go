package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

type NodeStatus string

const (
	NodeStatusUp      NodeStatus = "up"
	NodeStatusDown    NodeStatus = "down"
	NodeStatusLeaving NodeStatus = "leaving"
)

type NodeInfo struct {
	ID            string            `json:"id"`
	Status        NodeStatus        `json:"status"`
	Address       string            `json:"address"`
	WorkerCount   int               `json:"workerCount"`
	LastHeartbeat time.Time         `json:"lastHeartbeat"`
	IsLeader      bool              `json:"isLeader"`
	StartTime     time.Time         `json:"startTime"`
	Metadata      map[string]string `json:"metadata"`
}

type Node struct {
	Info              *NodeInfo
	redisClient       *redis.Client
	ctx               context.Context
	stopChan          chan struct{}
	heartbeatInterval time.Duration
	nodeExpiration    time.Duration
}

func NewNode(address string, redisClient *redis.Client) *Node {
	hostname, _ := os.Hostname()

	nodeID := fmt.Sprintf("%s-%s", hostname, uuid.New().String()[:8])

	return &Node{
		Info: &NodeInfo{
			ID:            nodeID,
			Status:        NodeStatusUp,
			Address:       address,
			StartTime:     time.Now(),
			LastHeartbeat: time.Now(),
			Metadata:      make(map[string]string),
		},
		redisClient:       redisClient,
		ctx:               context.Background(),
		stopChan:          make(chan struct{}),
		heartbeatInterval: 10 * time.Second,
		nodeExpiration:    30 * time.Second,
	}
}

func (n *Node) Start() error {

	if err := n.register(); err != nil {
		return fmt.Errorf("failed to register node: %w", err)
	}

	go n.heartbeatLoop()

	return nil
}

func (n *Node) Stop() error {

	close(n.stopChan)

	n.Info.Status = NodeStatusLeaving

	return n.updateInfo()
}

func (n *Node) register() error {
	nodeKey := fmt.Sprintf("node:%s", n.Info.ID)
	nodeJSON, err := json.Marshal(n.Info)
	if err != nil {
		return fmt.Errorf("error marshaling node info: %w", err)
	}

	pipe := n.redisClient.Pipeline()

	pipe.Set(n.ctx, nodeKey, nodeJSON, n.nodeExpiration)

	pipe.SAdd(n.ctx, "nodes", n.Info.ID)

	_, err = pipe.Exec(n.ctx)
	if err != nil {
		return fmt.Errorf("error registering node: %w", err)
	}

	return nil
}

func (n *Node) updateInfo() error {
	nodeKey := fmt.Sprintf("node:%s", n.Info.ID)
	nodeJSON, err := json.Marshal(n.Info)
	if err != nil {
		return fmt.Errorf("error marshaling node info: %w", err)
	}

	err = n.redisClient.Set(n.ctx, nodeKey, nodeJSON, n.nodeExpiration).Err()
	if err != nil {
		return fmt.Errorf("error updating node info: %w", err)
	}

	return nil
}

func (n *Node) heartbeatLoop() {
	ticker := time.NewTicker(n.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.Info.LastHeartbeat = time.Now()
			if err := n.updateInfo(); err != nil {
				fmt.Printf("Error sending heartbeat: %v\n", err)
			}

		case <-n.stopChan:
			return
		}
	}
}

func (n *Node) GetAllNodes() ([]*NodeInfo, error) {

	nodeIDs, err := n.redisClient.SMembers(n.ctx, "nodes").Result()
	if err != nil {
		return nil, fmt.Errorf("error retrieving node IDs: %w", err)
	}

	nodes := make([]*NodeInfo, 0, len(nodeIDs))

	for _, id := range nodeIDs {
		nodeKey := fmt.Sprintf("node:%s", id)
		nodeJSON, err := n.redisClient.Get(n.ctx, nodeKey).Result()

		if err != nil {
			if err == redis.Nil {

				n.redisClient.SRem(n.ctx, "nodes", id)
				continue
			}
			fmt.Printf("Error retrieving node %s: %v\n", id, err)
			continue
		}

		var nodeInfo NodeInfo
		if err := json.Unmarshal([]byte(nodeJSON), &nodeInfo); err != nil {
			fmt.Printf("Error unmarshaling node info: %v\n", err)
			continue
		}

		if time.Since(nodeInfo.LastHeartbeat) > n.nodeExpiration {
			nodeInfo.Status = NodeStatusDown

			n.redisClient.SRem(n.ctx, "nodes", id)
		}

		nodes = append(nodes, &nodeInfo)
	}

	return nodes, nil
}
