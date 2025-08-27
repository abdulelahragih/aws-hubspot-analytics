CREATE OR REPLACE VIEW hubspot_datalake.deal_kpis_monthly_v2 AS
WITH months AS (
  SELECT CAST(date_trunc('month', d) AS date) AS month_start
  FROM UNNEST(
    SEQUENCE(
      DATE '2025-03-01',
      DATE_TRUNC('month', CAST(current_timestamp AT TIME ZONE 'America/Montevideo' AS date)),
      INTERVAL '1' MONTH
    )
  ) AS t(d)
),
opportunities AS (
  SELECT
    CAST(date_trunc('month', at_timezone(op_detected_at, 'America/Montevideo')) AS date) AS month_start,
    COUNT(1) AS opportunities_created
  FROM hubspot_datalake.deals_latest
  WHERE op_detected_at IS NOT NULL
  GROUP BY 1
),
proposals AS (
  SELECT
    CAST(date_trunc('month', at_timezone(proposal_sent_at, 'America/Montevideo')) AS date) AS month_start,
    COUNT(1) AS proposals_sent
  FROM hubspot_datalake.deals_latest
  WHERE proposal_sent_at IS NOT NULL
  GROUP BY 1
),
closed_won AS (
  SELECT
    CAST(date_trunc('month', at_timezone(closed_won_at, 'America/Montevideo')) AS date) AS month_start,
    COUNT(1) AS closed_won
  FROM hubspot_datalake.deals_latest_clean
  WHERE closed_won_at IS NOT NULL
    AND deal_status_quality = 'properly_closed_won'
  GROUP BY 1
),
avg_op_to_prep AS (
  SELECT
    CAST(date_trunc('month', at_timezone(proposal_prep_at, 'America/Montevideo')) AS date) AS month_start,
    TRY(AVG(CASE
      WHEN date_diff('day',
           CAST(at_timezone(op_detected_at, 'America/Montevideo') AS date),
           CAST(at_timezone(proposal_prep_at, 'America/Montevideo') AS date)) >= 0
      THEN date_diff('day',
           CAST(at_timezone(op_detected_at, 'America/Montevideo') AS date),
           CAST(at_timezone(proposal_prep_at, 'America/Montevideo') AS date))
      ELSE NULL
    END)) AS avg_op_to_prep_days,
    COUNT_IF(op_detected_at IS NOT NULL AND proposal_prep_at IS NOT NULL) AS num_deals
  FROM hubspot_datalake.deals_latest
  WHERE op_detected_at IS NOT NULL AND proposal_prep_at IS NOT NULL
  GROUP BY 1
),
avg_prep_to_sent AS (
  SELECT
    CAST(date_trunc('month', at_timezone(proposal_sent_at, 'America/Montevideo')) AS date) AS month_start,
    TRY(AVG(CASE
      WHEN date_diff('day',
           CAST(at_timezone(proposal_prep_at, 'America/Montevideo') AS date),
           CAST(at_timezone(proposal_sent_at, 'America/Montevideo') AS date)) >= 0
      THEN date_diff('day',
           CAST(at_timezone(proposal_prep_at, 'America/Montevideo') AS date),
           CAST(at_timezone(proposal_sent_at, 'America/Montevideo') AS date))
      ELSE NULL
    END)) AS avg_prep_to_sent_days,
    COUNT_IF(proposal_prep_at IS NOT NULL AND proposal_sent_at IS NOT NULL) AS num_deals
  FROM hubspot_datalake.deals_latest
  WHERE proposal_prep_at IS NOT NULL AND proposal_sent_at IS NOT NULL
  GROUP BY 1
),
avg_sent_to_won AS (
  SELECT
    CAST(date_trunc('month', at_timezone(closed_won_at, 'America/Montevideo')) AS date) AS month_start,
    TRY(AVG(CASE
      WHEN date_diff('day',
           CAST(at_timezone(proposal_sent_at, 'America/Montevideo') AS date),
           CAST(at_timezone(closed_won_at, 'America/Montevideo') AS date)) >= 0
      THEN date_diff('day',
           CAST(at_timezone(proposal_sent_at, 'America/Montevideo') AS date),
           CAST(at_timezone(closed_won_at, 'America/Montevideo') AS date))
      ELSE NULL
    END)) AS avg_sent_to_won_days,
    COUNT_IF(proposal_sent_at IS NOT NULL AND closed_won_at IS NOT NULL) AS num_deals
  FROM hubspot_datalake.deals_latest_clean
  WHERE proposal_sent_at IS NOT NULL AND closed_won_at IS NOT NULL
    AND deal_status_quality = 'properly_closed_won'
  GROUP BY 1
),
avg_sales_cycle AS (
  SELECT
    CAST(date_trunc('month', at_timezone(closed_won_at, 'America/Montevideo')) AS date) AS month_start,
    TRY(AVG(CASE
      WHEN date_diff('day',
           CAST(at_timezone(created_at, 'America/Montevideo') AS date),
           CAST(at_timezone(closed_won_at, 'America/Montevideo') AS date)) >= 0
      THEN date_diff('day',
           CAST(at_timezone(created_at, 'America/Montevideo') AS date),
           CAST(at_timezone(closed_won_at, 'America/Montevideo') AS date))
      ELSE NULL
    END)) AS avg_sales_cycle_days,
    COUNT_IF(created_at IS NOT NULL AND closed_won_at IS NOT NULL) AS num_deals
  FROM hubspot_datalake.deals_latest_clean
  WHERE created_at IS NOT NULL AND closed_won_at IS NOT NULL
    AND deal_status_quality = 'properly_closed_won'
  GROUP BY 1
)
SELECT
  m.month_start,
  COALESCE(o.opportunities_created, 0) AS opportunities_created,
  COALESCE(p.proposals_sent, 0) AS proposals_sent,
  COALESCE(w.closed_won, 0) AS closed_won,
  a1.avg_op_to_prep_days,
  a2.avg_prep_to_sent_days,
  a3.avg_sent_to_won_days,
  a4.avg_sales_cycle_days
FROM months m
LEFT JOIN opportunities o ON o.month_start = m.month_start
LEFT JOIN proposals p ON p.month_start = m.month_start
LEFT JOIN closed_won w ON w.month_start = m.month_start
LEFT JOIN avg_op_to_prep a1 ON a1.month_start = m.month_start
LEFT JOIN avg_prep_to_sent a2 ON a2.month_start = m.month_start
LEFT JOIN avg_sent_to_won a3 ON a3.month_start = m.month_start
LEFT JOIN avg_sales_cycle a4 ON a4.month_start = m.month_start
ORDER BY m.month_start;



