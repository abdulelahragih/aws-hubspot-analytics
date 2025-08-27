CREATE OR REPLACE VIEW hubspot_datalake.deal_cohort_closing_rates AS
WITH cohorts AS (
  SELECT CAST(date_trunc('month', d) AS date) AS cohort_month
  FROM UNNEST(
    SEQUENCE(
      DATE_TRUNC('month', DATE_ADD('month', -5, CAST(current_timestamp AT TIME ZONE 'America/Montevideo' AS date))),
      DATE_TRUNC('month', CAST(current_timestamp AT TIME ZONE 'America/Montevideo' AS date)),
      INTERVAL '1' MONTH
    )
  ) AS t(d)
),
bounds AS (
  SELECT MIN(cohort_month) AS first_cohort FROM cohorts
),
months AS (
  SELECT CAST(date_trunc('month', d) AS date) AS month_start
  FROM bounds b
  CROSS JOIN UNNEST(
    SEQUENCE(
      b.first_cohort,
      DATE_TRUNC('month', CAST(current_timestamp AT TIME ZONE 'America/Montevideo' AS date)),
      INTERVAL '1' MONTH
    )
  ) AS t(d)
),
month_bounds AS (
  SELECT 
    cohort_month,
    -- Start of month: first day at 00:00:00 in Santiago, converted to UTC
    from_iso8601_timestamp(
      date_format(
        CAST(cohort_month AS timestamp) AT TIME ZONE 'America/Montevideo',
        '%Y-%m-%dT%H:%i:%sZ'
      )
    ) AS month_start_utc,
    -- End of month: last day at 23:59:59 in Santiago, converted to UTC
    from_iso8601_timestamp(
      date_format(
        (CAST(date_add('day', -1, date_add('month', 1, cohort_month)) AS timestamp) + INTERVAL '23' HOUR + INTERVAL '59' MINUTE + INTERVAL '59' SECOND) AT TIME ZONE 'America/Montevideo',
        '%Y-%m-%dT%H:%i:%sZ'
      )
    ) AS month_end_utc
  FROM cohorts
),
deals AS (
  SELECT
    d.deal_id,
    mb.cohort_month,
    -- Find which month this deal was closed (won OR lost)
    COALESCE(
      -- First try closed_won_at
      (SELECT mb2.cohort_month 
       FROM month_bounds mb2 
       WHERE d.closed_won_at BETWEEN mb2.month_start_utc AND mb2.month_end_utc),
      -- Then try closed_lost_at
      (SELECT mb2.cohort_month 
       FROM month_bounds mb2 
       WHERE d.closed_lost_at BETWEEN mb2.month_start_utc AND mb2.month_end_utc)
    ) AS closed_month
  FROM hubspot_datalake.deals_latest d
  CROSS JOIN month_bounds mb
  WHERE d.created_at BETWEEN mb.month_start_utc AND mb.month_end_utc
),
totals AS (
  SELECT cohort_month, COUNT(1) AS total_deals
  FROM deals
  WHERE cohort_month IS NOT NULL
  GROUP BY 1
),
closed AS (
  SELECT cohort_month, closed_month, COUNT(1) AS closed_in_month
  FROM deals
  WHERE cohort_month IS NOT NULL AND closed_month IS NOT NULL
  GROUP BY 1, 2
),
grid AS (
  SELECT c.cohort_month, m.month_start
  FROM cohorts c
  CROSS JOIN months m
  WHERE m.month_start >= c.cohort_month
),
joined AS (
  SELECT
    g.cohort_month,
    g.month_start,
    COALESCE(cl.closed_in_month, 0) AS closed_in_month,
    COALESCE(t.total_deals, 0) AS total_deals
  FROM grid g
  LEFT JOIN closed cl ON cl.cohort_month = g.cohort_month AND cl.closed_month = g.month_start
  LEFT JOIN totals t ON t.cohort_month = g.cohort_month
)
SELECT
  cohort_month,
  month_start,
  total_deals,
  closed_in_month,
  SUM(closed_in_month) OVER (PARTITION BY cohort_month ORDER BY month_start ROWS UNBOUNDED PRECEDING) AS cumulative_closed,
  CASE WHEN total_deals > 0 THEN CAST(ROUND(
    (SUM(closed_in_month) OVER (PARTITION BY cohort_month ORDER BY month_start ROWS UNBOUNDED PRECEDING) * 100.0) / total_deals
  ) AS integer) ELSE 0 END AS cumulative_pct
FROM joined
ORDER BY cohort_month, month_start;



