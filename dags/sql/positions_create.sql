CREATE TABLE IF NOT EXISTS positions_new (
    report_date DATE,
    client_account_id TEXT,
    asset_class TEXT,
    sub_category TEXT,
    description TEXT,
    cusip TEXT,
    isin TEXT,
    symbol TEXT,
    mark_price NUMERIC,
    quantity NUMERIC,
    fx_rate_to_base NUMERIC,
    PRIMARY KEY (report_date, client_account_id, symbol)
)
;
