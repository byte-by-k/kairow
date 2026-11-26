-- Example migration for an append-only outbox(id uuid, payload text, aggregate_id text, event_version bigint).
-- Install the normal Kairow schema first. Source IDs, keys and versions must be non-null.
BEGIN;
LOCK TABLE outbox IN SHARE ROW EXCLUSIVE MODE;
CREATE TABLE outbox_state (LIKE polling_job INCLUDING ALL);
ALTER TABLE outbox_state ADD COLUMN aggregate_id text NOT NULL, ADD COLUMN event_version bigint NOT NULL;
CREATE UNIQUE INDEX outbox_state_version ON outbox_state(aggregate_id,event_version);
CREATE FUNCTION kairow_capture_outbox() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO outbox_state(id,payload,aggregate_id,event_version,max_attempts)
    VALUES(NEW.id,NEW.payload,NEW.aggregate_id,NEW.event_version,5);
    RETURN NEW;
END;
$$;
CREATE TRIGGER kairow_capture AFTER INSERT ON outbox
FOR EACH ROW EXECUTE FUNCTION kairow_capture_outbox();
INSERT INTO outbox_state(id,payload,aggregate_id,event_version,max_attempts)
SELECT id,payload,aggregate_id,event_version,5 FROM outbox;
COMMIT;
