CREATE TABLE firefly_account_connection (
    id SERIAL PRIMARY KEY,
    account_id VARCHAR(255) NOT NULL,
    connection_id VARCHAR(255) NOT NULL,
    connection_name VARCHAR(255),
    connection_platform VARCHAR(255),
    wallet_addr VARCHAR(255),
    data_source VARCHAR(255),
    action VARCHAR(255) NOT NULL,
    update_time TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_account_connection UNIQUE (account_id, connection_id)
);