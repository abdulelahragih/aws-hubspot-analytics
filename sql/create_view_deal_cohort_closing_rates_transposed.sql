CREATE OR REPLACE VIEW hubspot_datalake.deal_cohort_closing_rates_transposed AS
WITH base_data AS (
  SELECT * FROM hubspot_datalake.deal_cohort_closing_rates
),
cohort_months AS (
  SELECT DISTINCT cohort_month FROM base_data ORDER BY cohort_month
),
closing_months AS (
  SELECT DISTINCT month_start FROM base_data ORDER BY month_start
),
transposed AS (
  SELECT 
    cm.cohort_month,
    clm.month_start,
    -- Create row labels matching the Google Scripts format
    'Deals created in ' || date_format(cm.cohort_month, '%M %Y') AS cohort_label,
    -- Create column labels  
    date_format(clm.month_start, '%M %Y') AS month_label,
    -- Get the cumulative percentage for this cohort-month combination
    COALESCE(bd.cumulative_pct, 0) AS cumulative_pct
  FROM cohort_months cm
  CROSS JOIN closing_months clm
  LEFT JOIN base_data bd 
    ON bd.cohort_month = cm.cohort_month 
    AND bd.month_start = clm.month_start
  -- Only show months that are >= cohort month (can't close before creation)
  WHERE clm.month_start >= cm.cohort_month
)
SELECT
  cohort_month,
  month_start,
  cohort_label,
  month_label,
  cumulative_pct,
  -- Add sorting helpers
  EXTRACT(year FROM cohort_month) AS cohort_year,
  EXTRACT(month FROM cohort_month) AS cohort_month_num,
  EXTRACT(year FROM month_start) AS closing_year,
  EXTRACT(month FROM month_start) AS closing_month_num
FROM transposed
ORDER BY cohort_month, month_start;
