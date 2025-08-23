CREATE OR REPLACE VIEW hubspot_datalake.deal_kpis_monthly_transposed AS
WITH monthly_data AS (
  SELECT * FROM hubspot_datalake.deal_kpis_monthly_v2
),
all_months AS (
  SELECT DISTINCT month_start FROM monthly_data ORDER BY month_start
),
kpi_labels AS (
  SELECT 1 AS sort_order, 'Opportunities Created' AS kpi_metric, 'count' AS value_type
  UNION ALL
  SELECT 2, 'Proposals Sent', 'count'
  UNION ALL  
  SELECT 3, 'Deals Closed (Won)', 'count'
  UNION ALL
  SELECT 4, 'Avg Days Op Detected → Proposal Prep', 'avg_days'
  UNION ALL
  SELECT 5, 'Avg Days Proposal Prep → Proposal Sent', 'avg_days'
  UNION ALL
  SELECT 6, 'Avg Days Proposal Sent → Won', 'avg_days'
  UNION ALL
  SELECT 7, 'Sales Cycle Avg (Create → Won)', 'avg_days'
),
transposed AS (
  -- Opportunities Created
  SELECT 
    kl.sort_order,
    kl.kpi_metric,
    md.month_start,
    CAST(md.opportunities_created AS double) AS kpi_value,
    kl.value_type
  FROM monthly_data md
  CROSS JOIN kpi_labels kl
  WHERE kl.kpi_metric = 'Opportunities Created'
  
  UNION ALL
  
  -- Proposals Sent
  SELECT 
    kl.sort_order,
    kl.kpi_metric,
    md.month_start,
    CAST(md.proposals_sent AS double) AS kpi_value,
    kl.value_type
  FROM monthly_data md
  CROSS JOIN kpi_labels kl
  WHERE kl.kpi_metric = 'Proposals Sent'
  
  UNION ALL
  
  -- Closed Won
  SELECT 
    kl.sort_order,
    kl.kpi_metric,
    md.month_start,
    CAST(md.closed_won AS double) AS kpi_value,
    kl.value_type
  FROM monthly_data md
  CROSS JOIN kpi_labels kl
  WHERE kl.kpi_metric = 'Deals Closed (Won)'
  
  UNION ALL
  
  -- Op Detected → Prep
  SELECT 
    kl.sort_order,
    kl.kpi_metric,
    md.month_start,
    md.avg_op_to_prep_days AS kpi_value,
    kl.value_type
  FROM monthly_data md
  CROSS JOIN kpi_labels kl
  WHERE kl.kpi_metric = 'Avg Days Op Detected → Proposal Prep'
  
  UNION ALL
  
  -- Prep → Sent
  SELECT 
    kl.sort_order,
    kl.kpi_metric,
    md.month_start,
    md.avg_prep_to_sent_days AS kpi_value,
    kl.value_type
  FROM monthly_data md
  CROSS JOIN kpi_labels kl
  WHERE kl.kpi_metric = 'Avg Days Proposal Prep → Proposal Sent'
  
  UNION ALL
  
  -- Sent → Won
  SELECT 
    kl.sort_order,
    kl.kpi_metric,
    md.month_start,
    md.avg_sent_to_won_days AS kpi_value,
    kl.value_type
  FROM monthly_data md
  CROSS JOIN kpi_labels kl
  WHERE kl.kpi_metric = 'Avg Days Proposal Sent → Won'
  
  UNION ALL
  
  -- Sales Cycle
  SELECT 
    kl.sort_order,
    kl.kpi_metric,
    md.month_start,
    md.avg_sales_cycle_days AS kpi_value,
    kl.value_type
  FROM monthly_data md
  CROSS JOIN kpi_labels kl
  WHERE kl.kpi_metric = 'Sales Cycle Avg (Create → Won)'
)
SELECT 
  sort_order,
  kpi_metric,
  month_start,
  date_format(month_start, '%M %Y') AS month_label,
  kpi_value,
  value_type,
  -- Add month sort order for QuickSight pivoting
  EXTRACT(year FROM month_start) AS sort_year,
  EXTRACT(month FROM month_start) AS sort_month
FROM transposed
ORDER BY sort_order, month_start;
