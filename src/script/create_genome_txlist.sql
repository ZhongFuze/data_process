CREATE TABLE genome_txlist (
    id SERIAL PRIMARY KEY,
    block_number BIGINT NOT NULL,
    block_timestamp BIGINT,
    from_address VARCHAR(255) NOT NULL,
    to_address VARCHAR(255) NOT NULL,
    tx_hash VARCHAR(255) NOT NULL UNIQUE,
    block_hash VARCHAR(255),
    nonce INT,
    transaction_index INT,
    tx_value TEXT,
    is_error BOOLEAN,
    txreceipt_status INT,
    contract_address VARCHAR(255),
    method_id TEXT,
    function_name TEXT,
    update_time TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_block_number ON genome_txlist (block_number);
CREATE INDEX idx_from_address ON genome_txlist (from_address);