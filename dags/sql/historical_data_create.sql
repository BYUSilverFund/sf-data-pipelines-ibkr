CREATE TABLE IF NOT EXISTS historical_data (
    report_date DATE,
    symbol TEXT,
    mark_price NUMERIC,
    daily_return NUMERIC,
    currency TEXT,
    PRIMARY KEY (report_date, symbol)
)
;
