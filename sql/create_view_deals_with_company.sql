CREATE OR REPLACE VIEW hubspot_datalake.deals_with_company AS
SELECT
  d.deal_id,
  d.deal_name,
  d.company_id,
  c.name AS company_name,
  d.owner_id,
  d.deal_stage,
  d.created_at,
  d.closed_at,
  d.amount,
  d.op_detected_at,
  d.proposal_prep_at,
  d.proposal_sent_at,
  d.closed_won_at,
  d.closed_lost_at,
  d.source,
  d.dt
FROM hubspot_datalake.deals_latest d
LEFT JOIN hubspot_datalake.companies c
  ON c.company_id = d.company_id;


