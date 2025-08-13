CREATE OR REPLACE VIEW hubspot_datalake.activity_volume_by_owner_type_last3weeks AS
WITH base AS (
  SELECT
    a.owner_id,
    COALESCE(o.owner_name, a.owner_id) AS owner_name,
    CAST(date_trunc('week', a.occurred_at) AS date) AS week_start,
    a.activity_type
  FROM hubspot_datalake.activities a
  LEFT JOIN hubspot_datalake.owners o
    ON o.owner_id = a.owner_id
)
SELECT
  owner_id,
  owner_name,
  week_start,
  activity_type,
  COUNT(1) AS activity_count
FROM base
WHERE week_start >= date_add('week', -2, CAST(date_trunc('week', current_date) AS date))
GROUP BY owner_id, owner_name, week_start, activity_type;


