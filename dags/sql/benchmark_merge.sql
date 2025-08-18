INSERT INTO benchmark_new (
    date,
    ticker,
    adjusted_close,
    return,
    dividends_per_share
)
SELECT
    date,
    ticker,
    adjusted_close,
    return,
    dividends_per_share
FROM "{{stage_table}}"
ON CONFLICT (date, ticker)
DO UPDATE SET 
    date = EXCLUDED.date,
    ticker = EXCLUDED.ticker,
    adjusted_close = EXCLUDED.adjusted_close,
    return = EXCLUDED.return,
    dividends_per_share = EXCLUDED.dividends_per_share
;
