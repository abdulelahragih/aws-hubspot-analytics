CREATE OR REPLACE VIEW hubspot_datalake.deal_kpis_weekly_last3_transposed AS
WITH base_data AS (
  SELECT * FROM hubspot_datalake.deal_kpis_weekly_last3
),
transposed AS (
  -- Opportunities Created
  SELECT 
    week_start,
    week_end,
    week_label,
    'Opportunities Created' AS kpi_metric,
    opportunities_created AS kpi_value,
    'count' AS value_type,
    1 AS sort_order
  FROM base_data
  
  UNION ALL
  
  -- Proposals Sent
  SELECT 
    week_start,
    week_end,
    week_label,
    'Proposals Sent' AS kpi_metric,
    proposals_sent AS kpi_value,
    'count' AS value_type,
    2 AS sort_order
  FROM base_data
  
  UNION ALL
  
  -- Deals Closed Won
  SELECT 
    week_start,
    week_end,
    week_label,
    'Deals Closed (Won)' AS kpi_metric,
    closed_won AS kpi_value,
    'count' AS value_type,
    3 AS sort_order
  FROM base_data
)
SELECT
  week_start,
  week_end,
  week_label,
  kpi_metric,
  kpi_value,
  value_type,
  sort_order,
  -- Additional analytics-friendly fields
  EXTRACT(year FROM week_start) AS sort_year,
  EXTRACT(week FROM week_start) AS sort_week,
  date_format(week_start, '%M %Y') AS month_label,
  -- Week position indicator (1 = most recent, 3 = oldest)  
  DENSE_RANK() OVER (ORDER BY week_start DESC) AS week_position
FROM transposed
ORDER BY week_start, sort_order;
