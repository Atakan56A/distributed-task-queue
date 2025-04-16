package config

import (
	"encoding/json"
	"os"
	"time"
)

type Config struct {
	QueueSize            int           `json:"queueSize"`
	LogLevel             string        `json:"logLevel"`
	RetryCount           int           `json:"retryCount"`
	WorkerCount          int           `json:"workerCount"`
	MinWorkers           int           `json:"minWorkers"`
	MaxWorkers           int           `json:"maxWorkers"`
	APIPort              int           `json:"apiPort"`
	SchedulerInterval    time.Duration `json:"schedulerInterval"`
	DataDir              string        `json:"dataDir"`
	AutoScalingEnabled   bool          `json:"autoScalingEnabled"`
	ScaleUpThreshold     float64       `json:"scaleUpThreshold"`
	ScaleDownThreshold   float64       `json:"scaleDownThreshold"`
	ScaleCheckInterval   int           `json:"scaleCheckInterval"`
	DistributionStrategy string        `json:"distributionStrategy"`
	ClusterEnabled       bool          `json:"clusterEnabled"`
	RedisAddress         string        `json:"redisAddress"`
	RedisPassword        string        `json:"redisPassword"`
	NodeAddress          string        `json:"nodeAddress"`
	HeartbeatInterval    int           `json:"heartbeatInterval"`
	NodeFailureTimeout   int           `json:"nodeFailureTimeout"`
	CleanRedisOnStart    bool          `json:"cleanRedisOnStart"`
}

func LoadConfig(filePath string) (*Config, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	config := &Config{
		QueueSize:            100,
		LogLevel:             "info",
		RetryCount:           3,
		WorkerCount:          5,
		MinWorkers:           2,
		MaxWorkers:           10,
		APIPort:              8080,
		SchedulerInterval:    1 * time.Second,
		DataDir:              "./data",
		AutoScalingEnabled:   true,
		ScaleUpThreshold:     0.7,
		ScaleDownThreshold:   0.3,
		ScaleCheckInterval:   30,
		DistributionStrategy: "least-loaded",
		CleanRedisOnStart:    true,
	}

	decoder := json.NewDecoder(file)
	err = decoder.Decode(config)

	if config.SchedulerInterval <= 0 {
		config.SchedulerInterval = 1 * time.Second
	}

	if config.MinWorkers <= 0 {
		config.MinWorkers = 1
	}

	if config.MaxWorkers < config.MinWorkers {
		config.MaxWorkers = config.MinWorkers * 2
	}

	if config.WorkerCount < config.MinWorkers {
		config.WorkerCount = config.MinWorkers
	}

	if config.WorkerCount > config.MaxWorkers {
		config.WorkerCount = config.MaxWorkers
	}

	return config, err
}
