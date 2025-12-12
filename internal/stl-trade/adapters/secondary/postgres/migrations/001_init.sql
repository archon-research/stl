CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(50) NOT NULL,
    amount DECIMAL(20, 8),
    side VARCHAR(10),
    status VARCHAR(20)
);
