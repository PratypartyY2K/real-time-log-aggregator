package embeddings

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

type OpenAIClient struct {
	APIKey     string
	Model      string
	Dimensions int
	URL        string
	HTTPClient *http.Client
}

func (c OpenAIClient) Embed(ctx context.Context, inputs []string) ([][]float64, error) {
	if strings.TrimSpace(c.APIKey) == "" {
		return nil, fmt.Errorf("OPENAI_API_KEY is required")
	}
	if len(inputs) == 0 {
		return nil, nil
	}
	if c.Model == "" {
		c.Model = "text-embedding-3-small"
	}
	if c.Dimensions <= 0 {
		c.Dimensions = 256
	}
	if c.URL == "" {
		c.URL = "https://api.openai.com/v1/embeddings"
	}
	if c.HTTPClient == nil {
		c.HTTPClient = &http.Client{Timeout: 30 * time.Second}
	}

	body, err := json.Marshal(map[string]any{
		"model":           c.Model,
		"input":           inputs,
		"dimensions":      c.Dimensions,
		"encoding_format": "float",
	})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(c.APIKey))
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("OpenAI embeddings returned %s: %s", resp.Status, bytes.TrimSpace(message))
	}

	var result struct {
		Data []struct {
			Embedding []float64 `json:"embedding"`
			Index     int       `json:"index"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	vectors := make([][]float64, len(inputs))
	for _, item := range result.Data {
		if item.Index < 0 || item.Index >= len(vectors) || len(item.Embedding) != c.Dimensions {
			return nil, fmt.Errorf("invalid embedding response")
		}
		vectors[item.Index] = item.Embedding
	}
	for _, vector := range vectors {
		if len(vector) == 0 {
			return nil, fmt.Errorf("missing embedding response")
		}
	}
	return vectors, nil
}
