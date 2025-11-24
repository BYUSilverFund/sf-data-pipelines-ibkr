INSERT INTO all_fund_returns (
    date,
    value,
    return,
    dividends
)
WITH filter AS(
    SELECT
        d.date,
        client_account_id,
        ending_value,
        deposits_withdrawals,
        dividends
    FROM delta_nav_new d
    INNER JOIN calendar_new c ON d.date = c.date
    WHERE client_account_id != 'DU8843649' -- Quant Paper Account
),
transform AS(
    SELECT
        date,
        client_account_id,
        COALESCE(LAG(ending_value) OVER (PARTITION BY client_account_id ORDER BY date), ending_value) AS starting_value,
        ending_value,
        deposits_withdrawals,
        dividends
    FROM filter
),
values AS(
    SELECT
        date,
        SUM(starting_value) AS starting_value,
        SUM(ending_value) AS ending_value,
        SUM(deposits_withdrawals) AS deposits_withdrawals,
        SUM(dividends) AS dividends
    FROM transform
    GROUP BY date
),
with_previous AS(
    SELECT
        date,
        starting_value,
        ending_value,
        deposits_withdrawals,
        dividends,
        LAG(ending_value) OVER (ORDER BY date) AS prev_ending_value
    FROM values
)
SELECT
    date,
    ending_value - deposits_withdrawals AS value,
    CASE 
        WHEN prev_ending_value IS NULL THEN NULL
        ELSE ((ending_value - deposits_withdrawals) / NULLIF(prev_ending_value, 0)) - 1
    END AS return,
    dividends
FROM with_previous
WHERE date BETWEEN '{{start_date}}' AND '{{end_date}}'
  AND prev_ending_value IS NOT NULL  -- Skip first date in window where LAG is NULL
ON CONFLICT (date)
DO UPDATE SET
    value = EXCLUDED.value,
    return = EXCLUDED.return,
    dividends = EXCLUDED.dividends
;