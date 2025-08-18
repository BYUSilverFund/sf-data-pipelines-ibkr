INSERT INTO delta_nav_new (
    date,
    client_account_id,
    starting_value,
    ending_value,
    deposits_withdrawals,
    dividends
)
SELECT
    date,
    client_account_id,
    starting_value,
    ending_value,
    deposits_withdrawals,
    dividends
FROM "{{stage_table}}"
ON CONFLICT (date, client_account_id)
DO UPDATE SET
    starting_value = EXCLUDED.starting_value,
    ending_value = EXCLUDED.ending_value,
    deposits_withdrawals = EXCLUDED.deposits_withdrawals,
    dividends = EXCLUDED.dividends
;
