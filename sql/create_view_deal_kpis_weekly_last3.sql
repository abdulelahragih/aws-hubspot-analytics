CREATE OR REPLACE VIEW hubspot_datalake.deal_kpis_weekly_last3 AS
WITH week_bounds AS (
  SELECT
    CASE
      WHEN day_of_week(current_date) = 7 THEN current_date
      ELSE date_add('day', -day_of_week(current_date), current_date)
    END AS last_sunday
),
last_3_weeks AS (
  SELECT
    CAST(date_add('day', -6, last_sunday) AS date) AS week1_start,
    CAST(last_sunday AS date) AS week1_end,
    CAST(date_add('day', -13, last_sunday) AS date) AS week2_start,
    CAST(date_add('day', -7, last_sunday) AS date) AS week2_end,
    CAST(date_add('day', -20, last_sunday) AS date) AS week3_start,
    CAST(date_add('day', -14, last_sunday) AS date) AS week3_end
  FROM week_bounds
),
date_bounds AS (
  SELECT week3_start AS first_week_start, week1_end AS last_week_end FROM last_3_weeks
),
weeks AS (
  SELECT CAST(date_trunc('week', d) AS date) AS week_start
  FROM date_bounds db
  CROSS JOIN UNNEST(SEQUENCE(
    db.first_week_start,
    db.last_week_end,
    INTERVAL '7' DAY
  )) AS t(d)
),
opportunities AS (
  SELECT
    w.week_start,
    COUNT(1) AS opportunities_created
  FROM weeks w
  LEFT JOIN hubspot_datalake.deals_latest d
    ON date_trunc('week', d.op_detected_at) = w.week_start
  WHERE d.op_detected_at IS NOT NULL
  GROUP BY 1
),
proposals AS (
  SELECT
    w.week_start,
    COUNT(1) AS proposals_sent
  FROM weeks w
  LEFT JOIN hubspot_datalake.deals_latest d
    ON date_trunc('week', d.proposal_sent_at) = w.week_start
  WHERE d.proposal_sent_at IS NOT NULL
  GROUP BY 1
),
won AS (
  SELECT
    w.week_start,
    COUNT(1) AS closed_won
  FROM weeks w
  LEFT JOIN hubspot_datalake.deals_latest d
    ON date_trunc('week', d.closed_won_at) = w.week_start
  WHERE d.closed_won_at IS NOT NULL
  GROUP BY 1
)
SELECT
  w.week_start,
  CAST(date_add('day', 6, w.week_start) AS date) AS week_end,
  date_format(w.week_start, '%b ') || CAST(day(w.week_start) AS varchar) ||
    ' - ' || date_format(date_add('day', 6, w.week_start), '%b ') || CAST(day(date_add('day', 6, w.week_start)) AS varchar) AS week_label,
  COALESCE(o.opportunities_created, 0) AS opportunities_created,
  COALESCE(p.proposals_sent, 0) AS proposals_sent,
  COALESCE(cw.closed_won, 0) AS closed_won
FROM weeks w
LEFT JOIN opportunities o ON o.week_start = w.week_start
LEFT JOIN proposals p ON p.week_start = w.week_start
LEFT JOIN won cw ON cw.week_start = w.week_start
ORDER BY w.week_start;



