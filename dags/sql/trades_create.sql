CREATE TABLE IF NOT EXISTS trades (
    report_date DATE,
    client_account_id TEXT,
    asset_class TEXT,
    sub_category TEXT,
    description TEXT,
    cusip TEXT,
    isin TEXT,
    symbol TEXT,
    trade_id TEXT,
    quantity NUMERIC,
    trade_price NUMERIC,
    ib_commission NUMERIC,
    buy_sell TEXT,
    trade_datetime TIMESTAMP,
    benchmark_price NUMERIC,
    PRIMARY KEY (report_date, client_account_id, symbol, trade_id)
);

ALTER TABLE trades ADD COLUMN IF NOT EXISTS trade_datetime TIMESTAMP;
ALTER TABLE trades ADD COLUMN IF NOT EXISTS benchmark_price NUMERIC;

