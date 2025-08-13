CREATE OR REPLACE VIEW hubspot_datalake.deal_kpis_monthly AS
WITH per_deal AS (
  SELECT * FROM hubspot_datalake.deal_stage_durations_per_deal
)
SELECT
  CAST(date_trunc('month', created_at) AS date) AS month_start,
  owner_id,
  owner_name,
  COUNT(*) AS deals_created,
  SUM(CASE WHEN is_won THEN 1 ELSE 0 END) AS deals_won,
  TRY(SUM(CASE WHEN is_won THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)) AS win_rate,
  TRY(AVG(TRY_CAST(sales_cycle_days AS double))) AS avg_sales_cycle_days,
  TRY(AVG(TRY_CAST(created_to_op_days AS double))) AS avg_created_to_op_days,
  TRY(AVG(TRY_CAST(op_to_prep_days AS double))) AS avg_op_to_prep_days,
  TRY(AVG(TRY_CAST(prep_to_sent_days AS double))) AS avg_prep_to_sent_days,
  TRY(AVG(TRY_CAST(sent_to_won_days AS double))) AS avg_sent_to_won_days,
  TRY(SUM(amount)) AS total_amount
FROM per_deal
GROUP BY 1, 2, 3
ORDER BY month_start DESC, owner_name;


