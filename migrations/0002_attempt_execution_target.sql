ALTER TABLE copy_attempts
ADD COLUMN execution_target TEXT NOT NULL DEFAULT 'surfpool'
CHECK (execution_target IN ('surfpool', 'mainnet'));

CREATE INDEX IF NOT EXISTS idx_copy_attempts_target_status
    ON copy_attempts(execution_target, status);
