CREATE TABLE genome_domains (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    tld_id INT NOT NULL,
    tld_name VARCHAR(255) NOT NULL,
    owner VARCHAR(255) NOT NULL,
    expired_at TIMESTAMP WITHOUT TIME ZONE,
    is_default BOOLEAN DEFAULT FALSE,
    token_id VARCHAR(255) NOT NULL,
    image_url TEXT,
    chain_id INT,
    action VARCHAR(255),
    create_time TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    delete_time BIGINT DEFAULT 0,
    CONSTRAINT unique_genome_domains UNIQUE (name, tld_name, owner)
);

CREATE INDEX idx_owner ON genome_domains (owner);
CREATE INDEX idx_tld_owner ON genome_domains (tld_name, owner);
CREATE INDEX idx_tld_name ON genome_domains (tld_name, name);
CREATE INDEX idx_tld_owner_query ON genome_domains (tld_name, owner, delete_time);
CREATE INDEX idx_tld_name_query ON genome_domains (tld_name, name, delete_time);

{
	'id': '14_1010915855723889042288748830868487681460570548751122716192574544312385431158',
	'name': 'caronfire',
	'tokenId': '1010915855723889042288748830868487681460570548751122716192574544312385431158',
	'owner': '0x782d7ff7214d3d9cb7a9afaf3f45a8f80cb73482',
	'expirationDate': 1747155857,
	'network': 14,
	'orderSource': 'SPACEID',
	'image': 'https://meta.image.space.id/images/mainnet/2702484275810670337286593638197304166435784191035983069259851825108946/1010915855723889042288748830868487681460570548751122716192574544312385431158.svg',
	'tld': {
		'tldID': 14,
		'tldName': 'gno',
		'chainID': '100'
	}
}