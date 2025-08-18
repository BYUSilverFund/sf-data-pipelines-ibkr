CREATE TABLE IF NOT EXISTS delta_nav_new (
    date DATE,
    client_account_id TEXT,
    starting_value NUMERIC,
    ending_value NUMERIC,
    deposits_withdrawals NUMERIC,
    dividends NUMERIC,
    PRIMARY KEY (date, client_account_id)
)
;
