CREATE TABLE clusters_name (
    id SERIAL PRIMARY KEY,
    bytes32Address VARCHAR(255) NOT NULL,
    address VARCHAR(255) NOT NULL,
    platform VARCHAR(255) NOT NULL,
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

UPDATE clusters_name
SET name = REPLACE(name, '"', '')
WHERE name LIKE '%"%';


CREATE TABLE clusters_events (
    id SERIAL PRIMARY KEY,
    clusterId VARCHAR(255),
    bytes32Address VARCHAR(255) NOT NULL,
    address VARCHAR(255) NOT NULL,
    addressType VARCHAR(255) NOT NULL,
    platform VARCHAR(255) NOT NULL,
    clusterName VARCHAR(255) NOT NULL,
    name VARCHAR(255),
    isVerified BOOLEAN DEFAULT FALSE,
    profileUrl text,
    imageUrl text,
    updatedAt TIMESTAMP WITHOUT TIME ZONE,
    CONSTRAINT unique_clusters_events UNIQUE (address, clusterName, name)
);

CREATE INDEX events_idx_address ON clusters_events (address);
CREATE INDEX events_idx_clusterName ON clusters_events (clusterName);

[{
	"name": "anfi/",
	"profileUrl": "https://clusters.xyz/anfi",
	"imageUrl": "https://cdn.clusters.xyz/profile/anfi",
	"hasCustomImage": false,
	"wallets": [{
		"name": "anfi/main",
		"type": "evm",
		"address": "0x6ff676b498eb4cf8b592c26a3462fd6c0ad484c5",
		"isVerified": true
	}, {
		"name": "anfi/celes",
		"type": "cosmos-celestia",
		"address": "celestia17gyaqsq2e0wx2shjl0c3lm66v7tdqlu38jc5s2",
		"isVerified": false
	}, {
		"name": "anfi/77f26",
		"type": "aptos",
		"address": "0x77f2604cde6942573aaf4001f8edf5be8bc9564e6fd2cc2da3765238813198d5",
		"isVerified": false
	}, {
		"name": "anfi/bcrus",
		"type": "solana",
		"address": "BcRUSXtaRNDnHW1i2uaeoKHPp6vs5BXuG6t64R3vczb1",
		"isVerified": false
	}]
}, {
	"name": "layerzero/",
	"profileUrl": "https://clusters.xyz/layerzero",
	"imageUrl": "https://cdn.clusters.xyz/profile/layerzero",
	"hasCustomImage": true,
	"wallets": [{
		"name": "layerzero/main",
		"type": "evm",
		"address": "0xccdead94e8cf17de32044d9701c4f5668ad0bef9",
		"isVerified": true
	}]
}]

{
  "nextPage": "8hzbf7V9wJ",
  "items": [
    {
        "eventType": "register",
        "clusterId": 420,
        "bytes32Address": "0x0000000000000000000000000e93e0bc42662dec41e9abdc718aa6d24c8e48b7",
        "address": "0x0e93e0bc42662dec41e9abdc718aa6d24c8e48b7",
        "addressType": "evm",
        "data": {
            "name": "00x",
            "weiAmount": 10000000000000000
        },
        "timestamp": 1706803811
    },
    {
        "eventType": "register",
        "clusterId": 474,
        "bytes32Address": "0x000000000000000000000000b4127fb17390504c5c3cf0bea1964269ae9914e7",
        "address": "0xb4127fb17390504c5c3cf0bea1964269ae9914e7",
        "addressType": "evm",
        "data": {
            "name": "200",
            "weiAmount": 10000000000000000
        },
        "timestamp": 1706803811
    },
    {
        "eventType": "register",
        "clusterId": 608,
        "bytes32Address": "0xa5172d0cfb7cfebb311bf80dde0ecc2f59f9f739536d0940ea34b222fbf84274",
        "address": "C7SmVULau6GWMNYxp3XaEMyyJZkZyjEfKdhU8HBX5LaF",
        "addressType": "solana",
        "data": {
                "name": "shelbs",
                "weiAmount": 9970674486803518
            },
        "timestamp": 1706803812
    },
    {
		"eventType": "updateWallet",
		"clusterId": 127227,
		"bytes32Address": "0x000000000000000000000000ff8c68d51ba3a77bf26d76c65f735bdd917955c1",
		"address": "0xff8c68d51ba3a77bf26d76c65f735bdd917955c1",
		"addressType": "evm",
		"data": {
			"name": "main",
			"isVerified": 1
		},
		"timestamp": 1721407292
	},
	{
		"eventType": "removeWallet",
		"clusterId": 127209,
		"bytes32Address": "0x00000000000000000000000000000000000e1a99dddd5610111884278bdbda1d",
		"address": "0x00000000000e1a99dddd5610111884278bdbda1d",
		"addressType": "evm",
		"data": null,
		"timestamp": 1721408346
	}
  ]
}