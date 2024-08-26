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