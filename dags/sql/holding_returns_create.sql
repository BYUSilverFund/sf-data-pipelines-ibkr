CREATE TABLE IF NOT EXISTS holding_returns (
    date DATE,
    client_account_id TEXT,
    ticker TEXT,
    weight NUMERIC,
    shares NUMERIC,
    price NUMERIC,
    value NUMERIC,
    shares_traded NUMERIC,
    average_trade_price NUMERIC,
    return NUMERIC,
    dividends NUMERIC,
    dividends_per_share NUMERIC,
    PRIMARY KEY (date, client_account_id, ticker)
)
;
