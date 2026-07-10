CREATE TABLE IF NOT EXISTS fraud_transactions (
    row_id BIGSERIAL PRIMARY KEY,
    transaction_id TEXT,
    amount DOUBLE PRECISION,
    location TEXT,
    event_time TIMESTAMP,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);