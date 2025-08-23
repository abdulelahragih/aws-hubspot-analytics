CREATE OR REPLACE VIEW hubspot_datalake.deal_kpis_weekly_last3_by_owner AS
WITH week_bounds AS (
  SELECT
    CASE
      WHEN day_of_week(CAST(current_timestamp AT TIME ZONE 'America/Santiago' AS date)) = 7 
      THEN CAST(current_timestamp AT TIME ZONE 'America/Santiago' AS date)
      ELSE date_add('day', -day_of_week(CAST(current_timestamp AT TIME ZONE 'America/Santiago' AS date)), 
                    CAST(current_timestamp AT TIME ZONE 'America/Santiago' AS date))
    END AS last_sunday
),
last_3_weeks AS (
  SELECT
    -- Current week (most recent): ends on last_sunday, starts 6 days before
    CAST(date_add('day', -6, last_sunday) AS date) AS current_week_start,
    CAST(last_sunday AS date) AS current_week_end,
    -- Previous week: ends 7 days before last_sunday, starts 13 days before
    CAST(date_add('day', -13, last_sunday) AS date) AS prev_week_start,
    CAST(date_add('day', -7, last_sunday) AS date) AS prev_week_end,
    -- Week before that: ends 14 days before last_sunday, starts 20 days before
    CAST(date_add('day', -20, last_sunday) AS date) AS prev2_week_start,
    CAST(date_add('day', -14, last_sunday) AS date) AS prev2_week_end
  FROM week_bounds
),
date_bounds AS (
  SELECT prev2_week_start AS first_week_start, current_week_end AS last_week_end FROM last_3_weeks
),
owners AS (
  SELECT owner_id, owner_name FROM hubspot_datalake.owners
),
weeks AS (
  SELECT DISTINCT CAST(d AS date) AS week_start
  FROM date_bounds db
  CROSS JOIN UNNEST(SEQUENCE(db.first_week_start, db.last_week_end, INTERVAL '7' DAY)) AS t(d)
),
opportunities AS (
  SELECT
    w.week_start,
    d.owner_id,
    COUNT(1) AS opportunities_created
  FROM weeks w
  LEFT JOIN hubspot_datalake.deals_latest d
    ON CAST(at_timezone(d.op_detected_at, 'America/Santiago') AS date) >= w.week_start
    AND CAST(at_timezone(d.op_detected_at, 'America/Santiago') AS date) <= date_add('day', 6, w.week_start)
  WHERE d.op_detected_at IS NOT NULL
  GROUP BY 1, 2
),
proposals AS (
  SELECT
    w.week_start,
    d.owner_id,
    COUNT(1) AS proposals_sent
  FROM weeks w
  LEFT JOIN hubspot_datalake.deals_latest d
    ON CAST(at_timezone(d.proposal_sent_at, 'America/Santiago') AS date) >= w.week_start
    AND CAST(at_timezone(d.proposal_sent_at, 'America/Santiago') AS date) <= date_add('day', 6, w.week_start)
  WHERE d.proposal_sent_at IS NOT NULL
  GROUP BY 1, 2
),
won AS (
  SELECT
    w.week_start,
    d.owner_id,
    COUNT(1) AS closed_won
  FROM weeks w
  LEFT JOIN hubspot_datalake.deals_latest_clean d
    ON CAST(at_timezone(d.closed_won_at, 'America/Santiago') AS date) >= w.week_start
    AND CAST(at_timezone(d.closed_won_at, 'America/Santiago') AS date) <= date_add('day', 6, w.week_start)
  WHERE d.closed_won_at IS NOT NULL 
    AND d.deal_status_quality = 'properly_closed_won'
  GROUP BY 1, 2
)
SELECT
  o.owner_id,
  COALESCE(os.owner_name, o.owner_id) AS owner_name,
  o.week_start,
  CAST(date_add('day', 6, o.week_start) AS date) AS week_end,
  -- Match Google Sheet label style: "Mon D - Sun D"
  date_format(o.week_start, '%b ') || CAST(day(o.week_start) AS varchar) ||
    ' - ' || date_format(date_add('day', 6, o.week_start), '%b ') || CAST(day(date_add('day', 6, o.week_start)) AS varchar) AS week_label,
  COALESCE(op.opportunities_created, 0) AS opportunities_created,
  COALESCE(pp.proposals_sent, 0) AS proposals_sent,
  COALESCE(ww.closed_won, 0) AS closed_won
FROM (
  SELECT owner_id, week_start FROM owners
  CROSS JOIN weeks
) o
LEFT JOIN owners os ON os.owner_id = o.owner_id
LEFT JOIN opportunities op ON op.owner_id = o.owner_id AND op.week_start = o.week_start
LEFT JOIN proposals pp ON pp.owner_id = o.owner_id AND pp.week_start = o.week_start
LEFT JOIN won ww ON ww.owner_id = o.owner_id AND ww.week_start = o.week_start
WHERE o.week_start BETWEEN (SELECT first_week_start FROM date_bounds) AND (SELECT last_week_end FROM date_bounds)
ORDER BY owner_name, o.week_start;



