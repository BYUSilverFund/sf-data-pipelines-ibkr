CREATE TABLE IF NOT EXISTS returns (
    report_date DATE,
    client_account_id TEXT,
    symbol TEXT,
    weight NUMERIC,
    shares INTEGER,
    close NUMERIC,
    return NUMERIC,
    dividends_per_share NUMERIC,
    PRIMARY KEY (report_date, client_account_id, symbol)
)
;
