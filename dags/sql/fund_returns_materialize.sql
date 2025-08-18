INSERT INTO fund_returns (
    date,
    client_account_id,
    ending_value,
    return,
    dividends
)
SELECT
    date,
    client_account_id,
    ending_value - deposits_withdrawals AS ending_value,
    (ending_value - deposits_withdrawals) / starting_value - 1 AS return,
    dividends
FROM delta_nav_new
WHERE date BETWEEN '{{start_date}}' AND '{{end_date}}'
ON CONFLICT (date, client_account_id)
DO UPDATE SET
    ending_value = EXCLUDED.ending_value,
    return = EXCLUDED.return,
    dividends = EXCLUDED.dividends
;
