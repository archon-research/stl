CREATE TABLE IF NOT EXISTS verification_results (
    id SERIAL PRIMARY KEY,
    model_id VARCHAR(255) NOT NULL,
    risk_score DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
