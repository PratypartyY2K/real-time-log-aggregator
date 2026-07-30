CREATE EXTENSION IF NOT EXISTS vector;

ALTER TABLE log_template_embeddings
    ADD COLUMN embedding vector(256);

UPDATE log_template_embeddings
SET embedding = embedding_json::text::vector;

ALTER TABLE log_template_embeddings
    ALTER COLUMN embedding SET NOT NULL,
    ADD CONSTRAINT log_template_embeddings_dimensions_256
        CHECK (embedding_dimensions = 256),
    DROP COLUMN embedding_json;
