INSERT INTO trades_new (
    report_date,
    client_account_id,
    asset_class,
    sub_category,
    description,
    cusip,
    isin,
    symbol,
    trade_id,
    quantity,
    trade_price,
    ib_commission,
    buy_sell
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
    trade_id,
    quantity,
    trade_price,
    ib_commission,
    buy_sell
FROM "{{stage_table}}"
ON CONFLICT (report_date, client_account_id, symbol, trade_id)
DO UPDATE SET
    asset_class = EXCLUDED.asset_class,
    sub_category = EXCLUDED.sub_category,
    description = EXCLUDED.description,
    cusip = EXCLUDED.cusip,
    isin = EXCLUDED.isin,
    quantity = EXCLUDED.quantity,
    trade_price = EXCLUDED.trade_price,
    ib_commission = EXCLUDED.ib_commission,
    buy_sell = EXCLUDED.buy_sell
;
