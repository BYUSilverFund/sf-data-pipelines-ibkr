INSERT INTO fund_returns (
    date,
    client_account_id,
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
)
SELECT
    date,
    client_account_id,
    ending_value - deposits_withdrawals AS value,
    (ending_value - deposits_withdrawals) / starting_value - 1 AS return,
    dividends
FROM transform
WHERE date BETWEEN '{{start_date}}' AND '{{end_date}}'
ON CONFLICT (date, client_account_id)
DO UPDATE SET
    value = EXCLUDED.value,
    return = EXCLUDED.return,
    dividends = EXCLUDED.dividends
;
