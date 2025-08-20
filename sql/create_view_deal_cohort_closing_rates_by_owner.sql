CREATE OR REPLACE VIEW hubspot_datalake.deal_cohort_closing_rates_by_owner AS
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
deal_base AS (
  SELECT
    d.deal_id,
    d.owner_id,
    CAST(date_trunc('month', d.created_at) AS date) AS cohort_month,
    CAST(date_trunc('month', d.closed_won_at) AS date) AS closed_month
  FROM hubspot_datalake.deals_latest d
),
owners_in_scope AS (
  SELECT DISTINCT owner_id FROM deal_base WHERE owner_id IS NOT NULL
),
totals AS (
  SELECT cohort_month, owner_id, COUNT(1) AS total_deals
  FROM deal_base
  WHERE cohort_month IS NOT NULL
  GROUP BY 1, 2
),
closed AS (
  SELECT cohort_month, owner_id, closed_month, COUNT(1) AS closed_in_month
  FROM deal_base
  WHERE cohort_month IS NOT NULL AND closed_month IS NOT NULL
  GROUP BY 1, 2, 3
),
grid AS (
  SELECT c.cohort_month, m.month_start, o.owner_id
  FROM cohorts c
  CROSS JOIN months m
  CROSS JOIN owners_in_scope o
  WHERE m.month_start >= c.cohort_month
),
joined AS (
  SELECT
    g.cohort_month,
    g.month_start,
    g.owner_id,
    COALESCE(cl.closed_in_month, 0) AS closed_in_month,
    COALESCE(t.total_deals, 0) AS total_deals
  FROM grid g
  LEFT JOIN closed cl ON cl.cohort_month = g.cohort_month AND cl.closed_month = g.month_start AND cl.owner_id = g.owner_id
  LEFT JOIN totals t ON t.cohort_month = g.cohort_month AND t.owner_id = g.owner_id
)
SELECT
  j.owner_id,
  COALESCE(o.owner_name, j.owner_id) AS owner_name,
  j.cohort_month,
  j.month_start,
  date_format(j.cohort_month, '%M %Y') AS cohort_label_en,
  date_format(j.month_start, '%M %Y') AS month_label_en,
  j.total_deals,
  j.closed_in_month,
  SUM(j.closed_in_month) OVER (PARTITION BY j.cohort_month, j.owner_id ORDER BY j.month_start ROWS UNBOUNDED PRECEDING) AS cumulative_closed,
  CASE WHEN j.total_deals > 0 THEN CAST(ROUND(
    (SUM(j.closed_in_month) OVER (PARTITION BY j.cohort_month, j.owner_id ORDER BY j.month_start ROWS UNBOUNDED PRECEDING) * 100.0) / j.total_deals
  ) AS integer) ELSE 0 END AS cumulative_pct
FROM joined j
LEFT JOIN hubspot_datalake.owners o ON o.owner_id = j.owner_id
ORDER BY owner_name, cohort_month, month_start;



