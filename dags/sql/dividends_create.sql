CREATE TABLE IF NOT EXISTS dividends_new (
    report_date DATE,
    client_account_id TEXT,
    asset_class TEXT,
    sub_category TEXT,
    description TEXT,
    cusip TEXT,
    isin TEXT,
    symbol TEXT,
    action_id TEXT,
    ex_date DATE,
    pay_date DATE,
    quantity NUMERIC,
    gross_rate NUMERIC,
    gross_amount NUMERIC,
    tax NUMERIC,
    fee NUMERIC,
    net_amount NUMERIC,
    PRIMARY KEY (report_date, client_account_id, symbol, action_id)
)
;
