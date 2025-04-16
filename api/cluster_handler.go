package api

import (
	"distributed-task-queue/internal/cluster"
	"distributed-task-queue/pkg/logger"
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

type ClusterHandler struct {
	Node          *cluster.Node
	LeaderElector *cluster.LeaderElector
	HealthMonitor *cluster.HealthMonitor
	Scheduler     *cluster.DistributedScheduler
	Logger        *logger.Logger
}

func NewClusterHandler(
	node *cluster.Node,
	leaderElector *cluster.LeaderElector,
	healthMonitor *cluster.HealthMonitor,
	scheduler *cluster.DistributedScheduler,
	logger *logger.Logger,
) *ClusterHandler {

	return &ClusterHandler{
		Node:          node,
		LeaderElector: leaderElector,
		HealthMonitor: healthMonitor,
		Scheduler:     scheduler,
		Logger:        logger,
	}
}

func (h *ClusterHandler) GetClusterNodes(w http.ResponseWriter, r *http.Request) {
	nodes, err := h.Node.GetAllNodes()
	if err != nil {
		h.Logger.Errorf("Failed to get cluster nodes: %v", err)
		http.Error(w, "Failed to retrieve cluster nodes", http.StatusInternalServerError)
		return
	}

	h.Logger.Info("Cluster nodes information requested")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(nodes)
}

func (h *ClusterHandler) GetNodeInfo(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	nodeID := vars["id"]

	nodes, err := h.Node.GetAllNodes()
	if err != nil {
		h.Logger.Errorf("Failed to get nodes: %v", err)
		http.Error(w, "Failed to retrieve nodes", http.StatusInternalServerError)
		return
	}

	for _, node := range nodes {
		if node.ID == nodeID {
			h.Logger.Infof("Node information requested: %s", nodeID)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(node)
			return
		}
	}

	h.Logger.Warnf("Node not found: %s", nodeID)
	http.Error(w, "Node not found", http.StatusNotFound)
}

func (h *ClusterHandler) GetNodeTasks(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	nodeID := vars["id"]

	tasks, err := h.Scheduler.GetNodeTasks(nodeID)
	if err != nil {
		h.Logger.Errorf("Failed to get tasks for node %s: %v", nodeID, err)
		http.Error(w, "Failed to retrieve node tasks", http.StatusInternalServerError)
		return
	}

	h.Logger.Infof("Node tasks requested for node: %s", nodeID)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tasks)
}

func (h *ClusterHandler) GetClusterStatus(w http.ResponseWriter, r *http.Request) {
	nodes, err := h.Node.GetAllNodes()
	if err != nil {
		h.Logger.Errorf("Failed to get cluster status: %v", err)
		http.Error(w, "Failed to retrieve cluster status", http.StatusInternalServerError)
		return
	}

	var activeNodes, downNodes int
	for _, node := range nodes {
		if node.Status == cluster.NodeStatusUp {
			activeNodes++
		} else if node.Status == cluster.NodeStatusDown {
			downNodes++
		}
	}

	leaderID, err := h.LeaderElector.GetCurrentLeader()
	if err != nil {
		leaderID = "Unknown"
	}

	status := map[string]interface{}{
		"nodes": map[string]int{
			"total":  len(nodes),
			"active": activeNodes,
			"down":   downNodes,
		},
		"thisNode": map[string]interface{}{
			"id":       h.Node.Info.ID,
			"address":  h.Node.Info.Address,
			"isLeader": h.LeaderElector.IsLeader(),
		},
		"leader": leaderID,
	}

	h.Logger.Info("Cluster status requested")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}
