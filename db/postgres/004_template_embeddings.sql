CREATE TABLE IF NOT EXISTS log_template_embeddings (
    tenant_id BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    template_id TEXT NOT NULL,
    message_template TEXT NOT NULL,
    embedding_model TEXT NOT NULL,
    embedding_dimensions INTEGER NOT NULL,
    embedding_json JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, template_id, embedding_model, embedding_dimensions)
);
