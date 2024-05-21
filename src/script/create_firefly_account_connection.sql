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


CREATE INDEX idx_platform_name ON firefly_account_connection (connection_platform, connection_name);
CREATE INDEX idx_wallet_addr ON firefly_account_connection (wallet_addr);
CREATE INDEX idx_action ON firefly_account_connection (action);

ALTER TABLE public.firefly_account_connection
ADD COLUMN display_name VARCHAR(255) DEFAULT '';
