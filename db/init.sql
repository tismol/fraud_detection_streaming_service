CREATE TABLE IF NOT EXISTS transaction_scores (
    id SERIAL PRIMARY KEY,
    transaction_id TEXT NOT NULL,
    score DOUBLE PRECISION NOT NULL,
    fraud_flag INTEGER NOT NULL,
    scored_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_transaction_scores_flag_time
    ON transaction_scores (fraud_flag, scored_at DESC);

CREATE INDEX IF NOT EXISTS idx_transaction_scores_time
    ON transaction_scores (scored_at DESC);
