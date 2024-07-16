SELECT * FROM ethereum.transaction_logs_decoded WHERE block_timestamp >= TIMESTAMP '{{start_time}}' AND block_timestamp < TIMESTAMP '{{end_time}}' AND contract_address IN ('0xd4416b13d2b3a9abae7acd5d6c2bbdbe25686401', '0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85', '0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e', '0x253553366da8546fc250f225fe3d25d0c782303b', '0x283af0b28c62c092c9727f1ee09c02ca627eb7f5', '0xdaaf96c344f63131acadd0ea35170e7892d3dfba', '0x4976fb03c32e5b8cfe2b6ccb31c09ba78ebaba41', '0x231b0ee14048e9dccd1d247744d114a4eb5e8e63', '0xa58e81fe9b61b5c3fe2afd33cf304c454abfc7cb') OFFSET {{custom_offset}} LIMIT {{custom_limit}}