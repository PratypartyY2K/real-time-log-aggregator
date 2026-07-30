ALTER TABLE logs_local ON CLUSTER logs_cluster
    ADD COLUMN IF NOT EXISTS message_template String AFTER message,
    ADD COLUMN IF NOT EXISTS template_id String AFTER message_template;

ALTER TABLE logs
    ADD COLUMN IF NOT EXISTS message_template String AFTER message,
    ADD COLUMN IF NOT EXISTS template_id String AFTER message_template;
