package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"sort"
	"strings"

	commonclickhouse "github.com/PratypartyY2K/real-time-log-aggregator/internal/clickhouse"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/config"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/embeddings"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type match struct {
	TemplateID      string     `json:"template_id"`
	MessageTemplate string     `json:"message_template"`
	Score           float64    `json:"score"`
	Logs            []evidence `json:"logs"`
	vector          []float64
}

type evidence struct {
	Timestamp  string `json:"timestamp"`
	Service    string `json:"service"`
	Level      string `json:"level"`
	Message    string `json:"message"`
	TraceID    string `json:"trace_id"`
	TemplateID string `json:"template_id"`
}

func main() {
	query := flag.String("query", "", "natural-language search query")
	tenantID := flag.Uint64("tenant-id", 1, "tenant ID")
	topK := flag.Int("top-k", 5, "number of matching templates")
	model := flag.String("model", "text-embedding-3-small", "OpenAI embedding model")
	dimensions := flag.Int("dimensions", 256, "embedding dimensions")
	flag.Parse()
	if strings.TrimSpace(*query) == "" || *tenantID == 0 || *topK <= 0 || *dimensions <= 0 {
		log.Fatal("query, tenant-id, top-k, and dimensions must be valid")
	}

	cfg := config.Load("search-templates", "")
	db, err := sql.Open("pgx", cfg.PostgresDSN)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	vectors, err := (embeddings.OpenAIClient{
		APIKey: os.Getenv("OPENAI_API_KEY"), Model: *model, Dimensions: *dimensions,
	}).Embed(ctx, []string{strings.TrimSpace(*query)})
	if err != nil {
		log.Fatal(err)
	}
	matches, err := rank(ctx, db, *tenantID, *model, *dimensions, vectors[0], *topK)
	if err != nil {
		log.Fatal(err)
	}
	if err := attachEvidence(ctx, cfg.ClickHouseDSN, *tenantID, matches); err != nil {
		log.Fatal(err)
	}
	if err := json.NewEncoder(os.Stdout).Encode(map[string]any{"query": *query, "matches": matches}); err != nil {
		log.Fatal(err)
	}
}

func rank(ctx context.Context, db *sql.DB, tenantID uint64, model string, dimensions int, query []float64, topK int) ([]match, error) {
	rows, err := db.QueryContext(ctx, `
SELECT template_id, message_template, embedding_json::text
FROM log_template_embeddings
WHERE tenant_id = $1 AND embedding_model = $2 AND embedding_dimensions = $3`,
		tenantID, model, dimensions)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// ponytail: exact scan is simplest for the initial template corpus; move cosine search to pgvector when measured latency requires it.
	matches := []match{}
	for rows.Next() {
		var item match
		var raw string
		if err := rows.Scan(&item.TemplateID, &item.MessageTemplate, &raw); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(raw), &item.vector); err != nil {
			return nil, err
		}
		item.Score = cosine(query, item.vector)
		matches = append(matches, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i].Score > matches[j].Score })
	if len(matches) > topK {
		matches = matches[:topK]
	}
	return matches, nil
}

func cosine(a, b []float64) float64 {
	if len(a) == 0 || len(a) != len(b) {
		return 0
	}
	var dot, aa, bb float64
	for i := range a {
		dot += a[i] * b[i]
		aa += a[i] * a[i]
		bb += b[i] * b[i]
	}
	if aa == 0 || bb == 0 {
		return 0
	}
	return dot / (math.Sqrt(aa) * math.Sqrt(bb))
}

func attachEvidence(ctx context.Context, clickhouseURL string, tenantID uint64, matches []match) error {
	if len(matches) == 0 {
		return nil
	}
	ids := make([]string, len(matches))
	byID := make(map[string]*match, len(matches))
	for i := range matches {
		ids[i] = "'" + strings.ReplaceAll(matches[i].TemplateID, "'", "''") + "'"
		byID[matches[i].TemplateID] = &matches[i]
	}
	query := fmt.Sprintf(`
SELECT timestamp, service, level, message, trace_id, template_id
FROM logs
WHERE tenant_id = %d AND template_id IN (%s)
ORDER BY timestamp DESC
LIMIT 100
FORMAT JSON`, tenantID, strings.Join(ids, ","))
	body, err := commonclickhouse.Do(ctx, http.DefaultClient, clickhouseURL, query)
	if err != nil {
		return err
	}
	defer body.Close()
	var response struct {
		Data []evidence `json:"data"`
	}
	if err := json.NewDecoder(body).Decode(&response); err != nil {
		return err
	}
	for _, item := range response.Data {
		if target := byID[item.TemplateID]; target != nil && len(target.Logs) < 3 {
			target.Logs = append(target.Logs, item)
		}
	}
	return nil
}
