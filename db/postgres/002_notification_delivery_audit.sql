ALTER TABLE notification_deliveries
    ADD COLUMN payload_json JSONB,
    ADD COLUMN max_attempts INTEGER NOT NULL DEFAULT 5,
    ADD COLUMN available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ADD COLUMN locked_at TIMESTAMPTZ,
    ADD COLUMN locked_by TEXT,
    ADD COLUMN sent_at TIMESTAMPTZ,
    ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

UPDATE notification_deliveries delivery
SET payload_json = COALESCE((
    SELECT event.payload_json
    FROM alert_events event
    WHERE event.alert_instance_id = delivery.alert_instance_id
      AND event.event_type = 'notification_enqueued'
      AND event.created_at <= delivery.created_at
    ORDER BY event.created_at DESC, event.id DESC
    LIMIT 1
), '{}'::JSONB);

UPDATE notification_deliveries
SET available_at = COALESCE(next_retry_at, created_at);

ALTER TABLE notification_deliveries
    ALTER COLUMN payload_json SET NOT NULL;

CREATE TABLE notification_delivery_attempts (
    id BIGSERIAL PRIMARY KEY,
    delivery_id BIGINT NOT NULL REFERENCES notification_deliveries(id) ON DELETE CASCADE,
    attempt_number INTEGER NOT NULL,
    status TEXT NOT NULL,
    error TEXT,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL,
    UNIQUE (delivery_id, attempt_number)
);

DROP INDEX IF EXISTS idx_notification_deliveries_status_retry;

CREATE INDEX idx_notification_deliveries_due
    ON notification_deliveries (status, available_at, id)
    WHERE status IN ('pending', 'retrying');

CREATE INDEX idx_notification_attempts_delivery
    ON notification_delivery_attempts (delivery_id, attempt_number);

CREATE INDEX idx_alert_events_instance_created
    ON alert_events (alert_instance_id, created_at DESC, id DESC);
