CREATE TABLE IF NOT EXISTS fund_returns (
    date DATE,
    client_account_id TEXT,
    value NUMERIC,
    return NUMERIC,
    dividends NUMERIC,
    PRIMARY KEY (date, client_account_id)
)
;
