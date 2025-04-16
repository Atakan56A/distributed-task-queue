package webhook

import (
	"bytes"
	"distributed-task-queue/pkg/logger"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type TaskInfo struct {
	ID          string      `json:"id"`
	Status      string      `json:"status"`
	Payload     interface{} `json:"payload"`
	CreatedAt   time.Time   `json:"createdAt"`
	CompletedAt time.Time   `json:"completedAt"`
	LastError   string      `json:"lastError,omitempty"`
}

type Webhook struct {
	ID          string            `json:"id"`
	URL         string            `json:"url"`
	Events      []string          `json:"events"`
	Headers     map[string]string `json:"headers"`
	Description string            `json:"description"`
	CreatedAt   time.Time         `json:"createdAt"`
}

type WebhookManager struct {
	webhooks map[string]*Webhook
	mu       sync.RWMutex
	client   *http.Client
	logger   *logger.Logger
}

func NewWebhookManager(log *logger.Logger) *WebhookManager {
	return &WebhookManager{
		webhooks: make(map[string]*Webhook),
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
		logger: log,
	}
}

func (wm *WebhookManager) RegisterWebhook(webhook *Webhook) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if webhook.ID == "" {
		return fmt.Errorf("webhook ID cannot be empty")
	}

	if webhook.URL == "" {
		return fmt.Errorf("webhook URL cannot be empty")
	}

	if len(webhook.Events) == 0 {
		return fmt.Errorf("webhook must specify at least one event")
	}

	if webhook.CreatedAt.IsZero() {
		webhook.CreatedAt = time.Now()
	}

	wm.webhooks[webhook.ID] = webhook
	wm.logger.Infof("Webhook registered: ID=%s, URL=%s", webhook.ID, webhook.URL)
	return nil
}

func (wm *WebhookManager) UnregisterWebhook(id string) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if _, exists := wm.webhooks[id]; !exists {
		return fmt.Errorf("webhook with ID %s not found", id)
	}

	delete(wm.webhooks, id)
	wm.logger.Infof("Webhook unregistered: ID=%s", id)
	return nil
}

func (wm *WebhookManager) GetWebhooks() []*Webhook {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	webhooks := make([]*Webhook, 0, len(wm.webhooks))
	for _, wh := range wm.webhooks {
		webhooks = append(webhooks, wh)
	}
	return webhooks
}

func (wm *WebhookManager) NotifyTaskEvent(task interface{}, event string) {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	var taskInfo TaskInfo

	if ti, ok := task.(TaskInfo); ok {
		taskInfo = ti
	} else {
		taskMap, ok := task.(map[string]interface{})
		if !ok {
			jsonData, err := json.Marshal(task)
			if err != nil {
				wm.logger.Errorf("Failed to convert task to JSON: %v", err)
				return
			}

			var taskMapFromJson map[string]interface{}
			if err := json.Unmarshal(jsonData, &taskMapFromJson); err != nil {
				wm.logger.Errorf("Failed to parse task JSON: %v", err)
				return
			}

			taskMap = taskMapFromJson
		}

		id, _ := taskMap["ID"].(string)
		status, _ := taskMap["Status"].(string)

		taskInfo = TaskInfo{
			ID:        id,
			Status:    status,
			Payload:   taskMap["Payload"],
			LastError: fmt.Sprintf("%v", taskMap["LastError"]),
		}

		if createdAt, ok := taskMap["CreatedAt"].(time.Time); ok {
			taskInfo.CreatedAt = createdAt
		}
		if completedAt, ok := taskMap["CompletedAt"].(time.Time); ok {
			taskInfo.CompletedAt = completedAt
		}
	}

	payload := map[string]interface{}{
		"taskId":      taskInfo.ID,
		"event":       event,
		"status":      taskInfo.Status,
		"timestamp":   time.Now(),
		"payload":     taskInfo.Payload,
		"createdAt":   taskInfo.CreatedAt,
		"completedAt": taskInfo.CompletedAt,
	}

	if taskInfo.LastError != "" {
		payload["error"] = taskInfo.LastError
	}

	for _, webhook := range wm.webhooks {
		subscribed := false
		for _, whEvent := range webhook.Events {
			if whEvent == event || whEvent == "all" {
				subscribed = true
				break
			}
		}

		if !subscribed {
			continue
		}

		go func(wh *Webhook, data map[string]interface{}) {
			wm.sendWebhookNotification(wh, data)
		}(webhook, payload)
	}
}

func (wm *WebhookManager) sendWebhookNotification(webhook *Webhook, data map[string]interface{}) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		wm.logger.Errorf("Failed to marshal webhook payload: %v", err)
		return
	}

	req, err := http.NewRequest("POST", webhook.URL, bytes.NewBuffer(jsonData))
	if err != nil {
		wm.logger.Errorf("Failed to create webhook request: %v", err)
		return
	}

	req.Header.Set("Content-Type", "application/json")
	for key, value := range webhook.Headers {
		req.Header.Set(key, value)
	}

	resp, err := wm.client.Do(req)
	if err != nil {
		wm.logger.Errorf("Failed to send webhook notification: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		wm.logger.Infof("Webhook notification sent successfully: ID=%s, URL=%s, StatusCode=%d",
			webhook.ID, webhook.URL, resp.StatusCode)
	} else {
		wm.logger.Warnf("Webhook notification failed: ID=%s, URL=%s, StatusCode=%d",
			webhook.ID, webhook.URL, resp.StatusCode)
	}
}
