INSERT INTO all_fund_returns (
    date,
    ending_value,
    return,
    dividends
)
WITH values AS(
    SELECT
        date,
        SUM(starting_value) AS starting_value,
        SUM(ending_value) AS ending_value,
        SUM(deposits_withdrawals) AS deposits_withdrawals,
        SUM(dividends) AS dividends
    FROM delta_nav_new
    GROUP BY date
)
SELECT
    date,
    ending_value - deposits_withdrawals AS ending_value,
    (ending_value - deposits_withdrawals) / starting_value - 1 AS return,
    dividends
FROM values
WHERE date BETWEEN '{{start_date}}' AND '{{end_date}}'
ON CONFLICT (date)
DO UPDATE SET
    ending_value = EXCLUDED.ending_value,
    return = EXCLUDED.return,
    dividends = EXCLUDED.dividends
;
