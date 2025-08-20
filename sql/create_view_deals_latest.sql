CREATE OR REPLACE VIEW hubspot_datalake.deals_latest AS
WITH ranked AS (
  SELECT
    d.*,
    ROW_NUMBER() OVER (
      PARTITION BY deal_id
      ORDER BY created_at DESC
    ) AS rn
  FROM hubspot_datalake.deals d
)
SELECT
  deal_id,
  deal_name,
  company_id,
  contact_id,
  owner_id,
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
  updated_at,
  dt
FROM ranked
WHERE rn = 1;


