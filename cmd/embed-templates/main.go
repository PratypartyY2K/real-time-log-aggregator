package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	commonclickhouse "github.com/PratypartyY2K/real-time-log-aggregator/internal/clickhouse"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/config"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/embeddings"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type template struct {
	TenantID        uint64 `json:"tenant_id"`
	TemplateID      string `json:"template_id"`
	MessageTemplate string `json:"template_text"`
}

func main() {
	model := flag.String("model", "text-embedding-3-small", "OpenAI embedding model")
	dimensions := flag.Int("dimensions", 256, "embedding dimensions")
	limit := flag.Int("limit", 100, "maximum new templates to embed")
	flag.Parse()
	if *dimensions <= 0 || *limit <= 0 {
		log.Fatal("dimensions and limit must be positive")
	}

	cfg := config.Load("embed-templates", "")
	db, err := sql.Open("pgx", cfg.PostgresDSN)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	templates, err := loadTemplates(ctx, cfg.ClickHouseDSN, *limit*10)
	if err != nil {
		log.Fatal(err)
	}
	pending, err := unindexed(ctx, db, templates, *model, *dimensions, *limit)
	if err != nil {
		log.Fatal(err)
	}
	if len(pending) == 0 {
		log.Print("no new templates to embed")
		return
	}

	client := embeddings.OpenAIClient{APIKey: os.Getenv("OPENAI_API_KEY"), Model: *model, Dimensions: *dimensions}
	inputs := make([]string, len(pending))
	for i := range pending {
		inputs[i] = pending[i].MessageTemplate
	}
	vectors, err := client.Embed(ctx, inputs)
	if err != nil {
		log.Fatal(err)
	}
	for i := range pending {
		payload, err := json.Marshal(vectors[i])
		if err != nil {
			log.Fatal(err)
		}
		_, err = db.ExecContext(ctx, `
INSERT INTO log_template_embeddings
    (tenant_id, template_id, message_template, embedding_model, embedding_dimensions, embedding_json)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, template_id, embedding_model, embedding_dimensions)
DO UPDATE SET message_template = EXCLUDED.message_template,
              embedding_json = EXCLUDED.embedding_json,
              updated_at = NOW()`,
			pending[i].TenantID, pending[i].TemplateID, pending[i].MessageTemplate, *model, *dimensions, string(payload))
		if err != nil {
			log.Fatal(err)
		}
	}
	log.Printf("embedded %d templates with %s (%d dimensions)", len(pending), *model, *dimensions)
}

func loadTemplates(ctx context.Context, clickhouseURL string, limit int) ([]template, error) {
	query := fmt.Sprintf(`
SELECT tenant_id, template_id, any(message_template) AS template_text
FROM logs
WHERE template_id != '' AND message_template != ''
GROUP BY tenant_id, template_id
ORDER BY tenant_id, template_id
LIMIT %d
SETTINGS output_format_json_quote_64bit_integers = 0
FORMAT JSON`, limit)
	body, err := commonclickhouse.Do(ctx, http.DefaultClient, clickhouseURL, query)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	var response struct {
		Data []template `json:"data"`
	}
	if err := json.NewDecoder(body).Decode(&response); err != nil {
		return nil, err
	}
	return response.Data, nil
}

func unindexed(ctx context.Context, db *sql.DB, templates []template, model string, dimensions, limit int) ([]template, error) {
	rows, err := db.QueryContext(ctx, `
SELECT tenant_id, template_id
FROM log_template_embeddings
WHERE embedding_model = $1 AND embedding_dimensions = $2`, model, dimensions)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	indexed := map[string]struct{}{}
	for rows.Next() {
		var tenantID uint64
		var templateID string
		if err := rows.Scan(&tenantID, &templateID); err != nil {
			return nil, err
		}
		indexed[fmt.Sprintf("%d:%s", tenantID, templateID)] = struct{}{}
	}
	pending := make([]template, 0, limit)
	for _, item := range templates {
		if _, ok := indexed[fmt.Sprintf("%d:%s", item.TenantID, item.TemplateID)]; !ok {
			pending = append(pending, item)
			if len(pending) == limit {
				break
			}
		}
	}
	return pending, rows.Err()
}
