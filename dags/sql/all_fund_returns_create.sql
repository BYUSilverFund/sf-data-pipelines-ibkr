CREATE TABLE IF NOT EXISTS all_fund_returns (
    date DATE,
    ending_value NUMERIC,
    return NUMERIC,
    dividends NUMERIC,
    PRIMARY KEY (date)
)
;
