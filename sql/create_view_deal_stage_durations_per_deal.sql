CREATE OR REPLACE VIEW hubspot_datalake.deal_stage_durations_per_deal AS
SELECT
  d.deal_id,
  d.deal_name,
  d.owner_id,
  COALESCE(o.owner_name, d.owner_id) AS owner_name,
  CAST(d.created_at AS timestamp) AS created_at,
  CAST(d.closed_at AS timestamp) AS closed_at,
  CAST(d.op_detected_at AS timestamp) AS op_detected_at,
  CAST(d.proposal_prep_at AS timestamp) AS proposal_prep_at,
  CAST(d.proposal_sent_at AS timestamp) AS proposal_sent_at,
  CAST(d.closed_won_at AS timestamp) AS closed_won_at,
  CAST(d.closed_lost_at AS timestamp) AS closed_lost_at,
  TRY_CAST(d.amount AS double) AS amount,
  TRY(date_diff('day', CAST(d.created_at AS timestamp), CAST(d.op_detected_at AS timestamp))) AS created_to_op_days,
  TRY(date_diff('day', CAST(d.op_detected_at AS timestamp), CAST(d.proposal_prep_at AS timestamp))) AS op_to_prep_days,
  TRY(date_diff('day', CAST(d.proposal_prep_at AS timestamp), CAST(d.proposal_sent_at AS timestamp))) AS prep_to_sent_days,
  TRY(date_diff('day', CAST(d.proposal_sent_at AS timestamp), CAST(d.closed_won_at AS timestamp))) AS sent_to_won_days,
  TRY(date_diff('day', CAST(d.created_at AS timestamp), COALESCE(CAST(d.closed_won_at AS timestamp), CAST(d.closed_lost_at AS timestamp), CAST(d.closed_at AS timestamp)))) AS sales_cycle_days,
  (d.closed_won_at IS NOT NULL) AS is_won
FROM hubspot_datalake.deals_latest d
LEFT JOIN hubspot_datalake.owners o
  ON o.owner_id = d.owner_id;


