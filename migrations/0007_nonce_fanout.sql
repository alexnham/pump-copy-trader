CREATE TABLE nonce_uses (
    account TEXT NOT NULL,
    nonce TEXT NOT NULL,
    source_signature TEXT NOT NULL,
    PRIMARY KEY(account, nonce)
);
CREATE TABLE copy_variants (
    source_signature TEXT NOT NULL REFERENCES source_transactions(signature),
    local_signature TEXT NOT NULL,
    signed_transaction BLOB NOT NULL,
    route_name TEXT NOT NULL,
    PRIMARY KEY(source_signature, local_signature)
);
