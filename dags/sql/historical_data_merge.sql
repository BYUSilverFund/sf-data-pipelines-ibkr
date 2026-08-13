INSERT INTO historical_data (
    report_date,
    symbol,
    mark_price,
    daily_return,
    currency
)
SELECT
    report_date,
    symbol,
    mark_price,
    daily_return,
    currency
FROM "{{stage_table}}"
ON CONFLICT (report_date, symbol)
DO UPDATE SET
    mark_price = EXCLUDED.mark_price,
    daily_return = EXCLUDED.daily_return,
    currency = EXCLUDED.currency
;
