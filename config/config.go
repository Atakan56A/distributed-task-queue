package config

import (
	"encoding/json"
	"os"
)

type Config struct {
	QueueSize   int    `json:"queue_size"`
	LogLevel    string `json:"log_level"`
	RetryCount  int    `json:"retry_count"`
	WorkerCount int    `json:"worker_count"`
	APIPort     int    `json:"api_port"`
}

func LoadConfig(filePath string) (*Config, error) {

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return &Config{
			QueueSize:   100,
			LogLevel:    "info",
			RetryCount:  3,
			WorkerCount: 5,
		}, nil
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	config := &Config{}
	err = decoder.Decode(config)
	if err != nil {
		return nil, err
	}

	return config, nil
}
