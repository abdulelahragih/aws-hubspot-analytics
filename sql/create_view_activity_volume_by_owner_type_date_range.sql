CREATE
OR REPLACE VIEW "activity_volume_by_owner_type_by_date_range" AS
WITH
  date_bounds AS (
    SELECT
      AT_TIMEZONE(TIMESTAMP '2025-05-01 00:00:00.000', 'America/Santiago') AS start_ts,
    AT_TIMEZONE(TIMESTAMP '2025-08-01 23:59:59.999', 'America/Santiago') AS end_ts
  ),
  allowed_owners AS (
    SELECT '47715895' AS owner_id
    UNION ALL SELECT '54166505'
    UNION ALL SELECT '81463777'
    UNION ALL SELECT '638599207'
    UNION ALL SELECT '1271887343'
  ),
  activity_events AS (
    SELECT
      a.owner_id,
      CASE
        WHEN UPPER(a.activity_type) = 'CALL' THEN 'Call'
        WHEN UPPER(a.activity_type) IN ('EMAIL', 'EMAIL_SENT', 'OUTBOUND_EMAIL', 'FORWARDED_EMAIL') THEN
          CASE
            WHEN UPPER(COALESCE(a.email_direction, '')) IN ('INCOMING_EMAIL', 'INCOMING', 'REPLY', 'REPLIED')
              OR LOWER(CAST(a.email_sent AS varchar)) = 'false'
            THEN 'Email reply from contact'
            ELSE 'Email sent to contact'
          END
        WHEN UPPER(a.activity_type) = 'INCOMING_EMAIL' THEN 'Email reply from contact'
        WHEN UPPER(a.activity_type) IN ('LINKEDIN_MESSAGE', 'LINKEDIN') THEN 'LinkedIn Message'
        WHEN UPPER(a.activity_type) IN ('SMS', 'TEXT', 'TEXT_MESSAGE') THEN 'SMS'
        WHEN UPPER(a.activity_type) IN ('WHATS_APP', 'WHATSAPP', 'WHATS-APP', 'WHATS APP') THEN 'WhatsApp'
        WHEN UPPER(a.activity_type) = 'MEETING' THEN 'Meeting'
        WHEN UPPER(a.activity_type) = 'TASK' THEN 'Task'
        WHEN UPPER(a.activity_type) = 'NOTE' THEN
          CASE
            WHEN LOWER(COALESCE(a.note_subject, '') || a.note_body || a.communication_body) LIKE '%linkedin%' THEN 'LinkedIn Message'
            WHEN LOWER(COALESCE(a.note_subject, '') || a.note_body || a.communication_body) LIKE '%whatsapp%' THEN 'WhatsApp'
            ELSE 'Note'
          END
        ELSE
          CASE
            WHEN LOWER(COALESCE(a.communication_source, '')) LIKE '%linkedin%' THEN 'LinkedIn Message'
            WHEN LOWER(COALESCE(a.communication_source, '')) LIKE '%whatsapp%' THEN 'WhatsApp'
            ELSE COALESCE(a.activity_type, 'Unknown')
          END
      END AS activity_category
    FROM hubspot_datalake.activities a
    INNER JOIN date_bounds db ON true
    INNER JOIN allowed_owners ao ON ao.owner_id = a.owner_id
    WHERE AT_TIMEZONE(a.occurred_at, 'America/Santiago')
            BETWEEN db.start_ts AND db.end_ts
      AND (
        a.dt IS NULL OR
        a.dt BETWEEN DATE_FORMAT(db.start_ts, '%Y-%m-%d') AND DATE_FORMAT(db.end_ts, '%Y-%m-%d')
      )
  ),
  contacts_created AS (
    SELECT
      c.owner_id,
      'Contact created' AS activity_category
    FROM hubspot_datalake.contacts c
    INNER JOIN date_bounds db ON true
    INNER JOIN allowed_owners ao ON ao.owner_id = c.owner_id
    WHERE AT_TIMEZONE(c.created_at, 'America/Santiago')
          BETWEEN db.start_ts AND db.end_ts
  ),
  contacts_worked AS (
    SELECT
      c.owner_id,
      'Contact worked' AS activity_category
    FROM hubspot_datalake.contacts c
    INNER JOIN date_bounds db ON true
    INNER JOIN allowed_owners ao ON ao.owner_id = c.owner_id
    WHERE AT_TIMEZONE(c.last_modified_at, 'America/Santiago')
          BETWEEN db.start_ts AND db.end_ts
  ),
  deals_created AS (
    SELECT
      d.owner_id,
      'Deal created' AS activity_category
    FROM hubspot_datalake.deals_latest d
    INNER JOIN date_bounds db ON true
    INNER JOIN allowed_owners ao ON ao.owner_id = d.owner_id
    WHERE AT_TIMEZONE(d.created_at, 'America/Santiago')
            BETWEEN db.start_ts AND db.end_ts
      AND (
        d.dt IS NULL OR
        d.dt BETWEEN DATE_FORMAT(db.start_ts, '%Y-%m-%d') AND DATE_FORMAT(db.end_ts, '%Y-%m-%d')
      )
  ),
  unioned AS (
    SELECT * FROM activity_events
    UNION ALL SELECT * FROM contacts_created
    UNION ALL SELECT * FROM contacts_worked
    UNION ALL SELECT * FROM deals_created
  ),
  final AS (
    SELECT
      u.owner_id,
      COALESCE(o.owner_name, u.owner_id) AS owner_name,
      u.activity_category AS activity_type
    FROM unioned u
    LEFT JOIN hubspot_datalake.owners o ON o.owner_id = u.owner_id
    WHERE u.activity_category IS NOT NULL
  )
SELECT owner_id,
       owner_name,
       activity_type,
       COUNT(1) AS activity_count
FROM final
GROUP BY owner_id, owner_name, activity_type;
