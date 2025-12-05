INSERT INTO risk_free_rate_new (
    date,
    return
)
SELECT
    date,
    return
FROM "{{stage_table}}"
ON CONFLICT (date)
DO UPDATE SET
    return = EXCLUDED.return
WHERE risk_free_rate_new.return IS DISTINCT FROM EXCLUDED.return
;