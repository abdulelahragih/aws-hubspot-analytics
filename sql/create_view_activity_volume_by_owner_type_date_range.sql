CREATE
OR REPLACE VIEW "activity_volume_by_owner" AS
WITH
  activity_events AS (
    SELECT
      a.owner_id,
      a.created_at,
      a.last_modified_at,
      (
        CASE
          WHEN (upper(a.activity_type) = 'CALL') THEN 'Call'
          WHEN (
            upper(a.activity_type) = 'EMAIL'
          ) THEN (
            CASE
              WHEN (
                (
                  upper(COALESCE(a.email_direction, '')) IN ('INCOMING_EMAIL')
                )
              ) THEN 'Email reply from contact' ELSE 'Email sent to contact'
            END
          )
          WHEN (upper(a.activity_type) = 'INCOMING_EMAIL') THEN 'Email reply from contact'
          WHEN (upper(a.activity_type) = 'FORWARDED_EMAIL') THEN 'Forwarded email'
          WHEN (
            upper(a.activity_type) IN ('LINKEDIN_MESSAGE', 'LINKEDIN')
          ) THEN 'LinkedIn Message'
          WHEN (
            upper(a.activity_type) = 'SMS'
          ) THEN 'SMS'
          WHEN (
            upper(a.activity_type) IN (
              'WHATS_APP',
              'WHATSAPP',
              'WHATS-APP',
              'WHATS APP'
            )
          ) THEN 'WhatsApp'
          WHEN (upper(a.activity_type) = 'MEETING') THEN 'Meeting'
          WHEN (upper(a.activity_type) = 'TASK') THEN 'Task'
          WHEN (upper(a.activity_type) = 'NOTE') THEN (
            CASE
              WHEN ((lower(COALESCE(a.note_body, '')) LIKE '%linkedin%') OR lower(COALESCE(a.communication_body, '')) LIKE '%linkedin%') THEN 'LinkedIn Message'
              WHEN ((lower(COALESCE(a.note_body, '')) LIKE '%whatsapp%') OR lower(COALESCE(a.communication_body, '')) LIKE '%whatsapp%') THEN 'WhatsApp' ELSE 'Note'
            END
          ) ELSE (
            CASE
              WHEN (
                lower(COALESCE(a.communication_source, '')) LIKE '%linkedin%'
              ) THEN 'LinkedIn Message'
              WHEN (
                lower(COALESCE(a.communication_source, '')) LIKE '%whatsapp%'
              ) THEN 'WhatsApp' ELSE COALESCE(a.activity_type, 'Unknown')
            END
          )
        END
      ) activity_category
    FROM hubspot_datalake.activities a
  ),
  contacts_created AS (
    SELECT
      c.owner_id,
      c.created_at,
      c.last_modified_at,
      'Contact created' activity_category
    FROM hubspot_datalake.contacts_75c422d9e8feb7b640a834d7f98a57f8 c
  ),
  contacts_worked AS (
    SELECT
      c.owner_id,
      c.last_modified_at as created_at,
      c.last_modified_at,
      'Contact worked' activity_category
    FROM hubspot_datalake.contacts_75c422d9e8feb7b640a834d7f98a57f8 c
  ),
  deals_created AS (
    SELECT
      d.owner_id,
      d.created_at,
      d.last_modified_at,
      'Deal created' activity_category
    FROM hubspot_datalake.deals_latest d
  ),
  unioned AS (
    SELECT owner_id, created_at, last_modified_at, activity_category
    FROM activity_events
    WHERE activity_category IS NOT NULL
    UNION ALL
    SELECT owner_id, created_at, last_modified_at, activity_category
    FROM contacts_created
    UNION ALL
    SELECT owner_id, created_at, last_modified_at, activity_category
    FROM contacts_worked
    UNION ALL
    SELECT owner_id, created_at, last_modified_at, activity_category
    FROM deals_created
  ),
  final AS (
    SELECT
      u.owner_id,
      COALESCE(o.owner_name, u.owner_id) owner_name,
      u.created_at,
      u.last_modified_at,
      u.activity_category activity_type
    FROM unioned u
    LEFT JOIN hubspot_datalake.owners o ON o.owner_id = u.owner_id
  )
SELECT owner_id,
       owner_name,
       created_at,
       last_modified_at,
       activity_type,
       COUNT(1) activity_count
FROM final
GROUP BY owner_id, owner_name, created_at, last_modified_at, activity_type;
