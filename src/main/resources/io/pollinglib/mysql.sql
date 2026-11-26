CREATE TABLE polling_job (
    id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    payload LONGTEXT NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'READY',
    attempts INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL CHECK (max_attempts > 0),
    available_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    owner_id TEXT,
    claim_token CHAR(36),
    INDEX polling_job_due (available_at,id)
) ENGINE=InnoDB;
CREATE TABLE polling_leader (
    queue_name VARCHAR(64) PRIMARY KEY,
    token CHAR(36),
    expires_at DATETIME(6)
) ENGINE=InnoDB;
INSERT INTO polling_leader(queue_name) VALUES ('polling_job');
