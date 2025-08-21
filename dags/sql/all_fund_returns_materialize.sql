INSERT INTO all_fund_returns (
    date,
    value,
    return,
    dividends
)
WITH transform AS(
    SELECT
        date,
        CASE
            WHEN starting_value = 0
            THEN ending_value
            ELSE starting_value
        END AS starting_value,
        ending_value,
        deposits_withdrawals,
        dividends
    FROM delta_nav_new
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
)
SELECT
    v.date,
    ending_value - deposits_withdrawals AS value,
    (ending_value - deposits_withdrawals) / starting_value - 1 AS return,
    dividends
FROM values v
INNER JOIN calendar_new c ON v.date = c.date
WHERE v.date BETWEEN '{{start_date}}' AND '{{end_date}}'
ON CONFLICT (date)
DO UPDATE SET
    value = EXCLUDED.value,
    return = EXCLUDED.return,
    dividends = EXCLUDED.dividends
;
