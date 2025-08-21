INSERT INTO fund_returns (
    date,
    client_account_id,
    value,
    return,
    dividends
)
WITH transform AS(
    SELECT
        date,
        client_account_id,
        CASE
            WHEN starting_value = 0
            THEN ending_value
            ELSE starting_value
        END AS starting_value,
        ending_value,
        deposits_withdrawals,
        dividends
    FROM delta_nav_new
)
SELECT
    t.date,
    client_account_id,
    ending_value - deposits_withdrawals AS value,
    (ending_value - deposits_withdrawals) / starting_value - 1 AS return,
    dividends
FROM transform t
INNER JOIN calendar_new c ON t.date = c.date
WHERE t.date BETWEEN '{{start_date}}' AND '{{end_date}}'
ON CONFLICT (date, client_account_id)
DO UPDATE SET
    value = EXCLUDED.value,
    return = EXCLUDED.return,
    dividends = EXCLUDED.dividends
;
