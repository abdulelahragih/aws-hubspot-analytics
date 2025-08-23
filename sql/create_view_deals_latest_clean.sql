CREATE OR REPLACE VIEW hubspot_datalake.deals_latest_clean AS
WITH ranked AS (
  SELECT
    d.*,
    ROW_NUMBER() OVER (
      PARTITION BY deal_id
      ORDER BY created_at DESC
    ) AS rn
  FROM hubspot_datalake.deals d
),
base_deals AS (
  SELECT 
    deal_id,
    deal_name,
    owner_id,
    company_id,
    contact_id,
    deal_stage,
    created_at,
    closed_at,
    last_modified_at,
    amount,
    op_detected_at,
    proposal_prep_at,
    proposal_sent_at,
    closed_won_at,
    closed_lost_at,
    source,
    dt
  FROM ranked
  WHERE rn = 1
)
SELECT 
  deal_id,
  deal_name,
  owner_id,
  company_id,
  contact_id,
  deal_stage,
  created_at,
  closed_at,
  last_modified_at,
  amount,
  op_detected_at,
  proposal_prep_at,
  proposal_sent_at,
  -- Clean closed_won_at: Only use if we have a proper stage transition date
  CASE 
    WHEN closed_won_at IS NOT NULL THEN closed_won_at
    ELSE NULL
  END AS closed_won_at,
  -- Clean closed_lost_at: Only use if we have a proper stage transition date  
  CASE
    WHEN closed_lost_at IS NOT NULL THEN closed_lost_at
    ELSE NULL
  END AS closed_lost_at,
  source,
  dt,
  -- Data quality flags for analysis
  CASE 
    WHEN closed_at IS NOT NULL AND closed_won_at IS NULL AND closed_lost_at IS NULL 
    THEN 'has_close_date_no_stage_transition'
    WHEN closed_won_at IS NOT NULL 
    THEN 'properly_closed_won'
    WHEN closed_lost_at IS NOT NULL 
    THEN 'properly_closed_lost'
    WHEN closed_at IS NOT NULL
    THEN 'closed_unknown_outcome'
    ELSE 'open'
  END AS deal_status_quality,
  -- Count of data quality issues
  CASE 
    WHEN closed_at IS NOT NULL AND closed_won_at IS NULL AND closed_lost_at IS NULL THEN 1
    ELSE 0
  END AS has_data_quality_issue
FROM base_deals;
