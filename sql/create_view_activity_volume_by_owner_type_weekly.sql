CREATE OR REPLACE VIEW hubspot_datalake.activity_volume_by_owner_type_weekly AS
SELECT
  a.owner_id,
  COALESCE(o.owner_name, a.owner_id) AS owner_name,
  CAST(date_trunc('week', a.occurred_at) AS date) AS week_start,
  a.activity_type,
  COUNT(1) AS activity_count
FROM hubspot_datalake.activities a
LEFT JOIN hubspot_datalake.owners o
  ON o.owner_id = a.owner_id
GROUP BY
  a.owner_id,
  COALESCE(o.owner_name, a.owner_id),
  CAST(date_trunc('week', a.occurred_at) AS date),
  a.activity_type;


