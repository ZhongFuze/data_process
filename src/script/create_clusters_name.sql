CREATE TABLE clusters_name (
    id SERIAL PRIMARY KEY,
    bytes32Address VARCHAR(255) NOT NULL,
    address VARCHAR(255) NOT NULL,
    type VARCHAR(255) NOT NULL,
    clusterName VARCHAR(255) NOT NULL,
    name VARCHAR(255),
    isVerified BOOLEAN DEFAULT FALSE,
    updatedAt TIMESTAMP WITHOUT TIME ZONE,
    CONSTRAINT unique_clusters_name UNIQUE (address, clusterName, name)
);

CREATE INDEX idx_address ON clusters_name (address);
CREATE INDEX idx_clusterName ON clusters_name (clusterName);


{
    "bytes32Address": "0x0000000000000000000000008ccb07293755004942f4451aeba897db44631061",
    "address": "0x8ccb07293755004942f4451aeba897db44631061",
    "type": "evm",
    "clusterName": "wagmiwiz",
    "name": "wagmiwiz/vault",
    "isVerified": false,
    "updatedAt": 1712871691
}


BEGIN;
TRUNCATE TABLE public.clusters_name;
ALTER SEQUENCE public.clusters_name_id_seq RESTART WITH 1;
COMMIT;