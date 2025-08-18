INSERT INTO positions_new (
    report_date,
    client_account_id,
    asset_class,
    sub_category,
    description,
    cusip,
    isin,
    symbol,
    mark_price,
    quantity,
    fx_rate_to_base
)
SELECT
    report_date,
    client_account_id,
    asset_class,
    sub_category,
    description,
    cusip,
    isin,
    symbol,
    mark_price,
    quantity,
    fx_rate_to_base
FROM "{{stage_table}}"
ON CONFLICT (report_date, client_account_id, symbol)
DO UPDATE SET
    asset_class = EXCLUDED.asset_class,
    sub_category = EXCLUDED.sub_category,
    description = EXCLUDED.description,
    cusip = EXCLUDED.cusip,
    isin = EXCLUDED.isin,
    mark_price = EXCLUDED.mark_price,
    quantity = EXCLUDED.quantity,
    fx_rate_to_base = EXCLUDED.fx_rate_to_base
;
