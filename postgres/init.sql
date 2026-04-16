CREATE TABLE IF NOT EXISTS order_metrics (
    id BIGSERIAL PRIMARY KEY,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    "user" VARCHAR(100) NOT NULL,
    order_count BIGINT NOT NULL,
    revenue NUMERIC(10, 2) NOT NULL,
    written_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_order_metrics_window ON order_metrics (window_start);
CREATE INDEX IF NOT EXISTS idx_order_metrics_user ON order_metrics ("user");

CREATE OR REPLACE VIEW latest_metrics AS
SELECT DISTINCT ON ("user")
    "user",
    window_start,
    window_end,
    order_count,
    revenue
FROM order_metrics
ORDER BY "user", window_start DESC;
