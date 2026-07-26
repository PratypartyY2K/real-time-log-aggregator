ALTER TABLE alert_rules
    ADD COLUMN IF NOT EXISTS log_level TEXT,
    ADD COLUMN IF NOT EXISTS fingerprint TEXT;

CREATE INDEX IF NOT EXISTS idx_alert_rules_model_scope
    ON alert_rules (tenant_id, status, service_id, log_level, fingerprint);
