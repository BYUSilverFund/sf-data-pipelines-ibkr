CREATE TABLE IF NOT EXISTS trades_new (
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
    PRIMARY KEY (report_date, client_account_id, symbol, trade_id)
)
;
