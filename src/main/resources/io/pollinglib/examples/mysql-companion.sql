-- Example migration for an append-only outbox(id char(36), payload longtext, aggregate_id varchar(128), event_version bigint).
-- Pause producers and pollers through this migration: MySQL DDL implicitly commits.
CREATE TABLE outbox_state LIKE polling_job;
ALTER TABLE outbox_state ADD COLUMN aggregate_id VARCHAR(128) NOT NULL,
    ADD COLUMN event_version BIGINT NOT NULL, ADD UNIQUE INDEX outbox_state_version(aggregate_id,event_version);
CREATE TRIGGER kairow_capture AFTER INSERT ON outbox FOR EACH ROW
    INSERT INTO outbox_state(id,payload,aggregate_id,event_version,max_attempts)
    VALUES(NEW.id,NEW.payload,NEW.aggregate_id,NEW.event_version,5);
INSERT INTO outbox_state(id,payload,aggregate_id,event_version,max_attempts)
SELECT id,payload,aggregate_id,event_version,5 FROM outbox;
INSERT INTO polling_leader(queue_name) VALUES ('outbox_state');
