-- Initialize the stock database
-- This script runs automatically when the stock-postgres container starts

-- Create the stock_data table
CREATE TABLE IF NOT EXISTS stock_data (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    open_price DECIMAL(10, 2),
    high_price DECIMAL(10, 2),
    low_price DECIMAL(10, 2),
    close_price DECIMAL(10, 2),
    volume BIGINT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, timestamp)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_stock_symbol_timestamp 
ON stock_data (symbol, timestamp);

CREATE INDEX IF NOT EXISTS idx_stock_created_at 
ON stock_data (created_at);

CREATE INDEX IF NOT EXISTS idx_stock_symbol 
ON stock_data (symbol);

-- Create a view for latest stock prices
CREATE OR REPLACE VIEW latest_stock_prices AS
SELECT DISTINCT ON (symbol) 
    symbol,
    timestamp,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    created_at
FROM stock_data
ORDER BY symbol, timestamp DESC;

-- Create a view for daily stock summaries
CREATE OR REPLACE VIEW daily_stock_summary AS
SELECT 
    symbol,
    DATE(timestamp) as trade_date,
    COUNT(*) as data_points,
    MIN(low_price) as day_low,
    MAX(high_price) as day_high,
    FIRST_VALUE(open_price) OVER (PARTITION BY symbol, DATE(timestamp) ORDER BY timestamp) as day_open,
    LAST_VALUE(close_price) OVER (PARTITION BY symbol, DATE(timestamp) ORDER BY timestamp) as day_close,
    SUM(volume) as total_volume,
    AVG(close_price) as avg_price
FROM stock_data
GROUP BY symbol, DATE(timestamp), open_price, close_price, timestamp
ORDER BY symbol, trade_date DESC;

-- Insert some sample data for testing (optional)
-- Uncomment the following lines if you want sample data

/*
INSERT INTO stock_data (symbol, timestamp, open_price, high_price, low_price, close_price, volume)
VALUES 
    ('TEST', '2024-01-01 10:00:00+00', 100.00, 105.00, 99.00, 102.50, 1000000),
    ('TEST', '2024-01-01 11:00:00+00', 102.50, 103.00, 101.00, 101.75, 800000),
    ('TEST', '2024-01-01 12:00:00+00', 101.75, 104.00, 101.50, 103.25, 1200000)
ON CONFLICT (symbol, timestamp) DO NOTHING;
*/

-- Create a function to get stock statistics
CREATE OR REPLACE FUNCTION get_stock_stats(stock_symbol TEXT)
RETURNS TABLE (
    symbol TEXT,
    total_records BIGINT,
    earliest_date TIMESTAMP WITH TIME ZONE,
    latest_date TIMESTAMP WITH TIME ZONE,
    avg_price DECIMAL(10,2),
    min_price DECIMAL(10,2),
    max_price DECIMAL(10,2)
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        stock_symbol::TEXT,
        COUNT(*),
        MIN(sd.timestamp),
        MAX(sd.timestamp),
        AVG(sd.close_price)::DECIMAL(10,2),
        MIN(sd.close_price),
        MAX(sd.close_price)
    FROM stock_data sd
    WHERE sd.symbol = stock_symbol
    GROUP BY sd.symbol;
END;
$$ LANGUAGE plpgsql;

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO stockuser;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO stockuser;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO stockuser;