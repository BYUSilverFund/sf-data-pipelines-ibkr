INSERT INTO calendar_new (
    date
)
SELECT
    date
FROM "{{stage_table}}"
ON CONFLICT (date)
DO NOTHING
;
