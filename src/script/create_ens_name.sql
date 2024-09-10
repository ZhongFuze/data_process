CREATE TABLE ens_name (
    id SERIAL PRIMARY KEY,
    namenode VARCHAR(66) NOT NULL,
    name VARCHAR(1024),
    label_name VARCHAR(1024),
    label VARCHAR(66),
    erc721_token_id VARCHAR(255),
    erc1155_token_id VARCHAR(255),
    parent_node VARCHAR(66),
    registration_time TIMESTAMP WITHOUT TIME ZONE,
    expire_time TIMESTAMP WITHOUT TIME ZONE,
    is_wrapped BOOLEAN DEFAULT FALSE,
    fuses INT,
    grace_period_ends TIMESTAMP WITHOUT TIME ZONE,
    owner VARCHAR(42),
    resolver VARCHAR(42),
    resolved_address VARCHAR(42),
    reverse_address VARCHAR(42),
    is_primary BOOLEAN DEFAULT FALSE,
    contenthash TEXT,
    update_time TIMESTAMP WITHOUT TIME ZONE,
    resolved_records JSONB default '{}'::jsonb,
    texts JSONB default '{}'::jsonb,
    CONSTRAINT unique_ens_name UNIQUE (namenode)
);

CREATE INDEX ens_name_index ON ens_name (name);
CREATE INDEX ens_label_name_index ON ens_name (label_name);
CREATE INDEX ens_name_owner_index ON ens_name (owner);
CREATE INDEX ens_name_resolved_index ON ens_name (resolved_address);
CREATE INDEX ens_name_reverse_index ON ens_name (reverse_address);



CREATE TABLE ens_record (
    id SERIAL PRIMARY KEY,
    block_timestamp TIMESTAMP WITHOUT TIME ZONE,
    namenode VARCHAR(66) NOT NULL,
    transaction_hash VARCHAR(66) NOT NULL,
    log_count INT NOT NULL,
    is_registered BOOLEAN DEFAULT FALSE,
    is_old_registered BOOLEAN DEFAULT FALSE,
    is_new_registered BOOLEAN DEFAULT FALSE,
    update_record TEXT,
    CONSTRAINT unique_ens_record UNIQUE (namenode, transaction_hash)
);

CREATE INDEX timestamp_ens_record_index ON ens_record (block_timestamp);

BEGIN;
TRUNCATE TABLE public.ens_record;
ALTER SEQUENCE public.ens_record_id_seq RESTART WITH 1;
COMMIT;