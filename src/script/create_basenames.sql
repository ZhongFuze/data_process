CREATE TABLE basenames (
    id SERIAL PRIMARY KEY,
    namenode VARCHAR(66) NOT NULL,
    name VARCHAR(1024),
    label VARCHAR(66),
    erc721_token_id VARCHAR(255),
    parent_node VARCHAR(66),
    registration_time TIMESTAMP WITHOUT TIME ZONE,
    expire_time TIMESTAMP WITHOUT TIME ZONE,
    grace_period_ends TIMESTAMP WITHOUT TIME ZONE,
    owner VARCHAR(42),
    resolver VARCHAR(42),
    resolved_address VARCHAR(42),
    reverse_address VARCHAR(42),
    contenthash TEXT,
    update_time TIMESTAMP WITHOUT TIME ZONE,
    resolved_records JSONB default '{}'::jsonb,
    key_value JSONB default '{}'::jsonb,
    CONSTRAINT unique_basenames UNIQUE (namenode)
);

CREATE INDEX basenames_index ON basenames (name);
CREATE INDEX basenames_owner_index ON basenames (owner);
CREATE INDEX basenames_resolved_index ON basenames (resolved_address);
CREATE INDEX basenames_reverse_index ON basenames (reverse_address);

ALTER TABLE public.basenames ADD COLUMN is_primary BOOLEAN DEFAULT FALSE;


CREATE TABLE basenames_record (
    id SERIAL PRIMARY KEY,
    block_timestamp TIMESTAMP WITHOUT TIME ZONE,
    namenode VARCHAR(66) NOT NULL,
    transaction_hash VARCHAR(66) NOT NULL,
    log_count INT NOT NULL,
    is_registered BOOLEAN DEFAULT FALSE,
    update_record TEXT,
    CONSTRAINT unique_basenames_record UNIQUE (namenode, transaction_hash)
);

CREATE INDEX timestamp_basenames_record_index ON basenames_record (block_timestamp);

BEGIN;
TRUNCATE TABLE public.basenames_record;
ALTER SEQUENCE public.basenames_record_id_seq RESTART WITH 1;
COMMIT;