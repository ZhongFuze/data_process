CREATE TABLE basenames_txlogs (
    id SERIAL PRIMARY KEY,
    block_number BIGINT NOT NULL,
    block_timestamp TIMESTAMP WITHOUT TIME ZONE,
    transaction_hash VARCHAR(66) NOT NULL,
    transaction_index INT,
    log_index INT,
    contract_address VARCHAR(42),
    contract_label VARCHAR(66),
    method_id VARCHAR(66),
    signature TEXT,
    decoded TEXT,
    CONSTRAINT unique_basenames_txlogs UNIQUE (transaction_hash, transaction_index, log_index)
);

CREATE INDEX basenames_txlogs_timestamp_index ON basenames_txlogs (block_timestamp);

BEGIN;
TRUNCATE TABLE public.basenames_txlogs;
ALTER SEQUENCE public.basenames_txlogs_id_seq RESTART WITH 1;
COMMIT;