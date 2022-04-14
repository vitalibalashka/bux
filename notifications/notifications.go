package notifications

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// GetWebhookEndpoint will get the configured webhook endpoint
func (c *Client) GetWebhookEndpoint() string {
	return c.options.config.webhookEndpoint
}

// Notify will create a new notification
func (c *Client) Notify(ctx context.Context, modelType string, eventType EventType,
	model interface{}, id string) error {

	if len(c.options.config.webhookEndpoint) == 0 {
		if c.IsDebug() {
			c.Logger().Info(ctx, fmt.Sprintf("NOTIFY %s: %s - %v", eventType, id, model))
		}
	} else {
		jsonData, err := json.Marshal(map[string]interface{}{
			"eventType": eventType,
			"id":        id,
			"model":     model,
			"modelType": modelType,
		})
		if err != nil {
			return err
		}

		var req *http.Request
		if req, err = http.NewRequestWithContext(ctx,
			http.MethodPost,
			c.options.config.webhookEndpoint,
			bytes.NewBuffer(jsonData),
		); err != nil {
			return err
		}

		var response *http.Response
		if response, err = c.options.httpClient.Do(req); err != nil {
			return err
		}
		defer func() {
			_ = response.Body.Close()
		}()

		if response.StatusCode != http.StatusOK {
			// todo queue notification for another try ...
			c.Logger().Error(ctx, fmt.Sprintf(
				"%s: %d",
				"received invalid response from notification endpoint: ",
				response.StatusCode))
		}
	}

	return nil
}
