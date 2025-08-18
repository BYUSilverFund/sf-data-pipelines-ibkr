INSERT INTO dividends_new (
    report_date,
    client_account_id,
    asset_class,
    sub_category,
    description,
    cusip,
    isin,
    symbol,
    action_id,
    ex_date,
    pay_date,
    quantity,
    gross_rate,
    gross_amount,
    tax,
    fee,
    net_amount
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
    action_id,
    ex_date,
    pay_date,
    quantity,
    gross_rate,
    gross_amount,
    tax,
    fee,
    net_amount
FROM "{{stage_table}}"
ON CONFLICT (report_date, client_account_id, symbol, action_id)
DO UPDATE SET
    asset_class = EXCLUDED.asset_class,
    sub_category = EXCLUDED.sub_category,
    description = EXCLUDED.description,
    cusip = EXCLUDED.cusip,
    isin = EXCLUDED.isin,
    ex_date = EXCLUDED.ex_date,
    pay_date = EXCLUDED.pay_date,
    quantity = EXCLUDED.quantity,
    gross_rate = EXCLUDED.gross_rate,
    gross_amount = EXCLUDED.gross_amount,
    tax = EXCLUDED.tax,
    fee = EXCLUDED.fee,
    net_amount = EXCLUDED.net_amount
;
