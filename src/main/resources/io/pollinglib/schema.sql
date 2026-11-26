CREATE TABLE polling_job (
    id uuid PRIMARY KEY,
    payload text NOT NULL,
    status text NOT NULL DEFAULT 'READY' CHECK (status IN ('READY','PROCESSING','DONE','FAILED')),
    attempts integer NOT NULL DEFAULT 0,
    max_attempts integer NOT NULL CHECK (max_attempts > 0),
    available_at timestamptz NOT NULL DEFAULT now(),
    owner_id text,
    claim_token uuid,
    CHECK ((status = 'PROCESSING') = (claim_token IS NOT NULL)),
    CHECK ((status = 'PROCESSING') = (owner_id IS NOT NULL))
);
CREATE INDEX polling_job_due ON polling_job (available_at, id)
    WHERE status IN ('READY','PROCESSING');
