CREATE TABLE keybase_proof (
    id SERIAL PRIMARY KEY,
    keybase_username VARCHAR(255),
    platform VARCHAR(255),
    username VARCHAR(255),
    display_name VARCHAR(255),
    proof_type INT,
    proof_state INT, 
    human_url TEXT,
    api_url TEXT,
    created_at TIMESTAMP WITHOUT TIME ZONE,
    record_id VARCHAR(255),
);