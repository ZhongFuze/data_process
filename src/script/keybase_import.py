#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import psycopg2
import setting

# Connect to your PostgreSQL database
conn = psycopg2.connect(setting.PG_DSN["keybase"])
conn.autocommit = True
cursor = conn.cursor()

# CREATE TABLE keybase_proof (
#     id SERIAL PRIMARY KEY,
#     keybase_username VARCHAR(255),
#     platform VARCHAR(255),
#     username VARCHAR(255),
#     display_name VARCHAR(255),
#     proof_type INT,
#     proof_state INT, 
#     human_url TEXT,
#     api_url TEXT,
#     created_at TIMESTAMP WITHOUT TIME ZONE,
#     record_id VARCHAR(255),
# );

# Specify the path to your local TSV file
file_path = '/path/keybase_proof.tsv'

# Open the TSV file for reading
with open(file_path, 'r', encoding="utf-8") as f:
    # Use copy_expert to copy data from the file to the target table
    # cursor.copy_expert(sql="COPY keybase_proof(keybase_username, platform, username, display_name, proof_type, proof_state, human_url, api_url, created_at) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\t', HEADER)", file=f)
    cursor.copy_expert(sql="COPY keybase_proof(keybase_username, platform, username, display_name, proof_type, proof_state, human_url, api_url, created_at, record_id) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\t', HEADER)", file=f)


# Clean up: close the cursor and connection
cursor.close()
conn.close()