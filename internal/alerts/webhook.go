package alerts

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const defaultWebhookTimeout = 10 * time.Second

type WebhookDispatcher struct {
	endpoint string
	client   *http.Client
}

type WebhookDeliveryPayload struct {
	DeliveryID      int64               `json:"delivery_id"`
	AlertInstanceID int64               `json:"alert_instance_id"`
	Notification    NotificationPayload `json:"notification"`
}

func NewWebhookDispatcher(endpoint string, client *http.Client) *WebhookDispatcher {
	if client == nil {
		client = &http.Client{Timeout: defaultWebhookTimeout}
	}
	return &WebhookDispatcher{endpoint: strings.TrimSpace(endpoint), client: client}
}

func (d *WebhookDispatcher) Dispatch(ctx context.Context, delivery NotificationDelivery, payload NotificationPayload) error {
	if d == nil || d.client == nil || d.endpoint == "" {
		return fmt.Errorf("webhook dispatcher is not configured")
	}

	body, err := json.Marshal(WebhookDeliveryPayload{
		DeliveryID:      delivery.ID,
		AlertInstanceID: delivery.AlertInstanceID,
		Notification:    payload,
	})
	if err != nil {
		return fmt.Errorf("marshal webhook notification: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build webhook request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "logagg-alerts/1.0")
	req.Header.Set("X-Logagg-Delivery-Id", fmt.Sprintf("%d", delivery.ID))
	req.Header.Set("X-Logagg-Alert-Instance-Id", fmt.Sprintf("%d", delivery.AlertInstanceID))

	resp, err := d.client.Do(req)
	if err != nil {
		return fmt.Errorf("post webhook notification: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		payload, readErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if readErr != nil {
			return fmt.Errorf("webhook notification failed with status %s", resp.Status)
		}
		return fmt.Errorf("webhook notification failed with status %s: %s", resp.Status, strings.TrimSpace(string(payload)))
	}
	return nil
}
