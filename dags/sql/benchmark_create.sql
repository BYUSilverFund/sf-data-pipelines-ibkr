CREATE TABLE IF NOT EXISTS benchmark_new (
    date DATE,
    ticker TEXT,
    adjusted_close NUMERIC,
    return NUMERIC,
    dividends_per_share NUMERIC,
    PRIMARY KEY (date, ticker)
)
;
