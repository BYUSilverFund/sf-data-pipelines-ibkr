INSERT INTO holding_returns (
    date,
    client_account_id,
    ticker,
    weight,
    shares,
    price,
    value,
    shares_traded,
    average_trade_price,
    return,
    dividends,
    dividends_per_share
)
WITH positions AS (
    SELECT
        client_account_id,
        report_date,
        symbol,
        fx_rate_to_base AS fx_rate_1,
        quantity AS shares_1,
        mark_price AS price_1,
        CASE WHEN quantity > 0 THEN 1 ELSE -1 END AS side
    FROM positions_new
    WHERE sub_category IN ('ETF', 'COMMON')
        AND report_date BETWEEN '{{start_date}}' AND '{{end_date}}'
),
dividends AS(
    SELECT
        client_account_id,
        report_date,
        symbol,
        SUM(net_amount) AS dividends,
        SUM(gross_rate) AS dividends_per_share
    FROM dividends_new
    WHERE sub_category IN ('ETF', 'COMMON')
        AND report_date BETWEEN '{{start_date}}' AND '{{end_date}}'
    GROUP BY client_account_id, symbol, report_date
),
trades AS(
    SELECT
        client_account_id,
        report_date,
        symbol,
        SUM(quantity) AS shares_traded,
        SUM(quantity * trade_price) / SUM(quantity) AS average_trade_price
    FROM trades_new
    WHERE sub_category IN ('ETF', 'COMMON')
        AND report_date BETWEEN '{{start_date}}' AND '{{end_date}}'
    GROUP BY client_account_id, symbol, report_date
    HAVING SUM(quantity) != 0
),
merge AS(
    SELECT
        base.client_account_id,
        base.report_date,
        base.symbol,
        p.side,
        p.fx_rate_1,
        p.shares_1,
        p.price_1,
        COALESCE(d.dividends, 0) AS dividends,
        COALESCE(d.dividends_per_share, 0) AS dividends_per_share,
        COALESCE(t.shares_traded, 0) AS shares_traded,
        COALESCE(t.average_trade_price, 0) AS average_trade_price
    FROM (
        SELECT DISTINCT client_account_id, report_date, symbol FROM positions
        UNION
        SELECT DISTINCT client_account_id, report_date, symbol FROM trades
    ) base
    LEFT JOIN positions p ON base.client_account_id = p.client_account_id
        AND base.report_date = p.report_date
        AND base.symbol = p.symbol
    LEFT JOIN dividends d ON base.client_account_id = d.client_account_id
        AND base.report_date = d.report_date
        AND base.symbol = d.symbol
    LEFT JOIN trades t ON base.client_account_id = t.client_account_id
        AND base.report_date = t.report_date
        AND base.symbol = t.symbol
),
shift AS (
    SELECT
        client_account_id,
        report_date,
        symbol,
        COALESCE(side, -SIGN(shares_traded)) AS side,
        COALESCE(fx_rate_1, 0) AS fx_rate_1,
        COALESCE(shares_1, 0) AS shares_1,
        COALESCE(price_1, average_trade_price) AS price_1,
        COALESCE(dividends, 0) AS dividends,
        COALESCE(dividends_per_share, 0) AS dividends_per_share,
        COALESCE(shares_traded, 0) AS shares_traded,
        COALESCE(average_trade_price, 0) AS average_trade_price,
        COALESCE(LAG(fx_rate_1) OVER (PARTITION BY client_account_id, symbol ORDER BY report_date), fx_rate_1) AS fx_rate_0,
        COALESCE(LAG(shares_1) OVER (PARTITION BY client_account_id, symbol ORDER BY report_date), shares_1) AS shares_0,
        COALESCE(LAG(price_1) OVER (PARTITION BY client_account_id, symbol ORDER BY report_date), price_1) AS price_0
    FROM merge
),
adjustments AS(
    SELECT
        client_account_id,
        report_date,
        symbol,
        side,
        shares_0,
        shares_1,
        shares_traded,
        average_trade_price,
        (
            CASE
                WHEN shares_1 - shares_traded = 0 -- Initial trade
                THEN shares_1
                WHEN shares_1 = 0-- Exit trade
                THEN shares_0
                ELSE shares_1 - shares_traded -- Trim positions
            END
        ) AS shares_1_adj,
        (
            CASE
                WHEN shares_1 - shares_traded = 0 -- Initial trade
                THEN average_trade_price * fx_rate_0
                ELSE price_0 * fx_rate_0 -- Normal
            END
        ) AS price_0,
        (
            CASE
                WHEN shares_1 = 0-- Exit trade
                THEN average_trade_price * fx_rate_0 -- Instead of coalescing sx_rate_1
                ELSE price_1 * fx_rate_1
            END
        ) AS price_1,
        dividends,
        dividends_per_share
    FROM shift
),
returns AS(
    SELECT
        client_account_id,
        report_date AS date,
        symbol AS ticker,
        (shares_1 * price_1) / SUM(shares_1 * price_1) OVER (PARTITION BY client_account_id, report_date) AS weight,
        (
            CASE
                WHEN shares_1 = 0 -- Exit trade
                THEN shares_0
                ELSE shares_1
            END
        ) AS shares,
        price_1 AS price,
        shares_traded,
        average_trade_price,
        (shares_1_adj * price_1 + dividends) / (shares_0 * price_0) - 1 AS return,
        dividends,
        dividends_per_share
    FROM adjustments a
    INNER JOIN calendar_new c ON a.report_date = c.date
)
SELECT 
    date,
    client_account_id,
    ticker,
    weight,
    shares,
    price,
    shares * price AS value,
    shares_traded,
    average_trade_price,
    return,
    dividends,
    dividends_per_share
FROM returns
ON CONFLICT (date, client_account_id, ticker)
DO UPDATE SET
    weight = EXCLUDED.weight, 
    shares = EXCLUDED.shares, 
    price = EXCLUDED.price, 
    value = EXCLUDED.value, 
    shares_traded = EXCLUDED.shares_traded,
    average_trade_price = EXCLUDED.average_trade_price,
    return = EXCLUDED.return, 
    dividends = EXCLUDED.dividends, 
    dividends_per_share = EXCLUDED.dividends_per_share
;
