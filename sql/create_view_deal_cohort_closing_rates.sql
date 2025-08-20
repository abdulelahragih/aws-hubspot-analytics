CREATE OR REPLACE VIEW hubspot_datalake.deal_cohort_closing_rates AS
WITH cohorts AS (
  SELECT CAST(date_trunc('month', d) AS date) AS cohort_month
  FROM UNNEST(
    SEQUENCE(
      DATE_TRUNC('month', DATE_ADD('month', -5, CURRENT_DATE)),
      DATE_TRUNC('month', CURRENT_DATE),
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
      DATE_TRUNC('month', CURRENT_DATE),
      INTERVAL '1' MONTH
    )
  ) AS t(d)
),
deals AS (
  SELECT
    deal_id,
    CAST(date_trunc('month', created_at) AS date) AS cohort_month,
    CAST(date_trunc('month', closed_won_at) AS date) AS closed_month
  FROM hubspot_datalake.deals_latest
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
  date_format(cohort_month, '%M %Y') AS cohort_label_en,
  date_format(month_start, '%M %Y') AS month_label_en,
  total_deals,
  closed_in_month,
  SUM(closed_in_month) OVER (PARTITION BY cohort_month ORDER BY month_start ROWS UNBOUNDED PRECEDING) AS cumulative_closed,
  CASE WHEN total_deals > 0 THEN CAST(ROUND(
    (SUM(closed_in_month) OVER (PARTITION BY cohort_month ORDER BY month_start ROWS UNBOUNDED PRECEDING) * 100.0) / total_deals
  ) AS integer) ELSE 0 END AS cumulative_pct
FROM joined
ORDER BY cohort_month, month_start;



