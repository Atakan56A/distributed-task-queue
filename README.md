# Distributed Task Queue

A robust, scalable, distributed task processing system implemented in Go. This system supports both standalone and cluster modes, allowing for resilient task distribution, processing, and monitoring.

## 📋 Features

- **Dual Mode Operation**: Run as standalone or in cluster mode with Redis
- **Task Management**:
  - Task creation, monitoring, and completion tracking
  - Support for recurring tasks
  - Priority-based task scheduling
  - Task timeout handling
  - Dead letter handling for failed tasks
- **Distributed Architecture**:
  - Leader election for coordinated scheduling
  - Task distribution across multiple nodes
  - Node health monitoring
  - Automatic failover
- **Resilience**:
  - Distributed task locking to prevent duplicate processing
  - Task completion tracking
  - Automatic Redis reconnection
  - Graceful shutdown
- **Monitoring & Metrics**:
  - Built-in metrics collection
  - Processing status reporting
  - Detailed logging
- **API**:
  - RESTful API for task management
  - Cluster status monitoring

## 🏗️ Architecture

The system consists of these core components:

- **Queue**: Central task storage and management
- **Scheduler**: Schedules and distributes tasks to workers
- **Worker Pool**: Processes tasks concurrently
- **Storage**: Pluggable storage backends (File/Redis)
- **Cluster**: Node management, leader election, health monitoring
- **API**: HTTP interface for interacting with the system

## 🚀 Getting Started

### Prerequisites

- Go 1.16+
- Redis (for cluster mode)

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/distributed-task-queue.git
cd distributed-task-queue

# Build the project
go build -o taskqueue ./cmd/taskqueueservice
```

### Configuration

Create a config.json file:

```json
{
  "queueSize": 100,
  "logLevel": "info",
  "retryCount": 3,
  "workerCount": 5,
  "apiPort": 8080,
  "dataDir": "./data",
  "clusterEnabled": false,
  "redisAddress": "localhost:6379",
  "redisPassword": "",
  "cleanRedisOnStart": false
}
```

For cluster mode, set `clusterEnabled` to `true` and provide valid Redis configuration.

### Running

```bash
# Standalone mode
./taskqueue

# With specific config
./taskqueue -config path/to/config.json
```

## 📖 API Reference

### Task Management

#### Create a Task

```
POST /api/tasks
```

Request body:
```json
{
  "payload": "Your task data here",
  "parameters": {
    "priority": "high",
    "timeout": "30s"
  }
}
```

Response:
```json
{
  "id": "task-uuid",
  "status": "pending",
  "createdAt": "2025-04-16T12:00:00Z"
}
```

#### List Tasks

```
GET /api/tasks
```

#### Get Task Details

```
GET /api/tasks/{taskId}
```

### Cluster Management

#### Get Cluster Status

```
GET /api/cluster/status
```

#### List Nodes

```
GET /api/cluster/nodes
```

## 🔧 Implementation Details

### Task States

A task can be in one of these states:
- `pending`: Ready to be processed
- `processing`: Currently being processed by a worker
- `completed`: Successfully processed
- `failed`: Processing failed but can be retried
- `dead-lettered`: Failed permanently

### Distributed Locking

To prevent duplicate processing in cluster mode, the system implements distributed locking with Redis:

1. Worker attempts to acquire a lock on a task
2. If successful, the task is processed
3. On completion, the lock is released
4. Stale locks are automatically cleaned up after a timeout

### Queue Processing

Tasks flow through the system as follows:

1. Created tasks are added to the queue
2. Scheduler fetches pending tasks
3. Tasks are distributed to available nodes
4. Worker processes the task
5. Task status is updated upon completion

## 🛠️ Troubleshooting

### Common Issues

#### Tasks Not Distributed

If tasks are not being distributed:
- Verify Redis connection
- Check for stale entries in the processing set
- Ensure nodes are properly registered

#### Redis Connection Errors

When seeing Redis connection errors:
- Verify Redis server is running
- Check network connectivity
- Review authentication settings

#### Worker Failures

If workers fail to process tasks:
- Check for task timeouts
- Review worker logs for errors
- Ensure task handlers are properly implemented

## 🧪 Testing

The system can be tested using the included API:

```bash
# Add a task
curl -X POST http://localhost:8080/api/tasks \
  -H "Content-Type: application/json" \
  -d '{"payload": "Test task", "parameters": {"priority": "high"}}'

# Check task status
curl http://localhost:8080/api/tasks/{task-id}
```

## 🔒 Security Considerations

- Redis connections should be secured with authentication and TLS in production
- API endpoints should be protected with authentication in production deployments
- Payload data should be validated to prevent injection attacks