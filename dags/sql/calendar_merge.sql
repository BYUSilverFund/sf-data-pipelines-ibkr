INSERT INTO calendar (
    date
)
SELECT
    date
FROM "{{stage_table}}"
ON CONFLICT (date)
DO NOTHING
;
