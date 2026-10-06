PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    surfpool_run_id TEXT NOT NULL,
    config_fingerprint TEXT NOT NULL,
    started_at INTEGER NOT NULL,
    stopped_at INTEGER
);

CREATE TABLE IF NOT EXISTS stream_cursor (
    wallet TEXT PRIMARY KEY,
    last_signature TEXT,
    last_slot INTEGER,
    connection_generation INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS source_transactions (
    signature TEXT PRIMARY KEY,
    slot INTEGER NOT NULL,
    block_time INTEGER,
    origin TEXT NOT NULL,
    status TEXT NOT NULL,
    dex TEXT,
    pool TEXT,
    raw_payload TEXT,
    skip_reason TEXT,
    observed_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS copy_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_signature TEXT NOT NULL UNIQUE,
    local_signature TEXT,
    signed_transaction BLOB,
    input_amount TEXT,
    minimum_output TEXT,
    simulation_json TEXT,
    status TEXT NOT NULL,
    error TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    FOREIGN KEY(source_signature) REFERENCES source_transactions(signature)
);

CREATE TABLE IF NOT EXISTS seeded_balances (
    surfpool_run_id TEXT NOT NULL,
    mint TEXT NOT NULL,
    token_account TEXT NOT NULL,
    amount TEXT NOT NULL,
    seeded_at INTEGER NOT NULL,
    PRIMARY KEY(surfpool_run_id, mint)
);

CREATE INDEX IF NOT EXISTS idx_source_transactions_slot
    ON source_transactions(slot);

CREATE INDEX IF NOT EXISTS idx_copy_attempts_status
    ON copy_attempts(status);
