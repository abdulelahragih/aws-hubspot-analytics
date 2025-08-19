CREATE
OR REPLACE VIEW hubspot_datalake.activity_volume_by_owner_type_last3weeks_v2 AS
WITH week_bounds AS (
  SELECT
    -- Find the most recent Sunday (end of current or previous week)
    CASE
      WHEN day_of_week(current_date) = 7 THEN current_date  -- Today is Sunday
      ELSE date_add('day', -(day_of_week(current_date)), current_date)  -- Go back to previous Sunday
    END AS last_sunday
),
last_3_weeks AS (
  SELECT
    -- Week 1 (most recent): last_sunday - 6 days (Monday) to last_sunday (Sunday)
    CAST(date_add('day', -6, last_sunday) AS date) AS week1_start,
    CAST(last_sunday AS date) AS week1_end,
    -- Week 2: 7 days before week 1
    CAST(date_add('day', -13, last_sunday) AS date) AS week2_start,
    CAST(date_add('day', -7, last_sunday) AS date) AS week2_end,
    -- Week 3 (oldest): 14 days before week 1
    CAST(date_add('day', -20, last_sunday) AS date) AS week3_start,
    CAST(date_add('day', -14, last_sunday) AS date) AS week3_end
  FROM week_bounds
),
date_bounds AS (
  SELECT
    week3_start AS first_week_start,
    week1_end AS last_week_end,
    week3_start AS start_date,
    week1_end AS end_date
  FROM last_3_weeks
),
allowed_owners AS (
  SELECT '47715895'   AS owner_id UNION ALL
  SELECT '54166505'   UNION ALL
  SELECT '81463777'   UNION ALL
  SELECT '638599207'  UNION ALL
  SELECT '1271887343'
),
activity_events AS (
  SELECT
    a.owner_id,
    CAST(date_trunc('week', a.occurred_at) AS date) AS week_start,
    CASE
  -- CALL
  WHEN upper(a.activity_type) = 'CALL' THEN 'Call'

  -- EMAIL family (treat FORWARDED_EMAIL as "sent to contact" to match the Sheet)
  WHEN upper(a.activity_type) IN ('EMAIL','EMAIL_SENT','OUTBOUND_EMAIL','FORWARDED_EMAIL') THEN
    CASE
      WHEN upper(coalesce(a.email_direction, '')) IN ('INCOMING_EMAIL','INCOMING','REPLY','REPLIED')
           OR lower(cast(a.email_sent AS varchar)) = 'false'
        THEN 'Email reply from contact'
      ELSE 'Email sent to contact'
    END

  -- Explicit incoming
  WHEN upper(a.activity_type) = 'INCOMING_EMAIL' THEN 'Email reply from contact'

  -- LinkedIn (base)
  WHEN upper(a.activity_type) IN ('LINKEDIN_MESSAGE','LINKEDIN') THEN 'LinkedIn Message'

  -- SMS
  WHEN upper(a.activity_type) IN ('SMS','TEXT','TEXT_MESSAGE') THEN 'SMS'

  -- WhatsApp (accept both spellings)
  WHEN upper(a.activity_type) IN ('WHATS_APP','WHATSAPP','WHATS-APP','WHATS APP') THEN 'WhatsApp'

  -- MEETING
  WHEN upper(a.activity_type) = 'MEETING' THEN 'Meeting'

  -- TASK
  WHEN upper(a.activity_type) = 'TASK' THEN 'Task'

  -- NOTE: content-based detection (also scan communication body just in case)
  WHEN upper(a.activity_type) = 'NOTE' THEN
    CASE
      WHEN lower(coalesce(a.note_subject, ''))    LIKE '%linkedin%'
        OR lower(coalesce(a.note_body, ''))       LIKE '%linkedin%'
        OR lower(coalesce(a.communication_body,'')) LIKE '%linkedin%'
        THEN 'LinkedIn Message'
      WHEN lower(coalesce(a.note_subject, ''))    LIKE '%whatsapp%'
        OR lower(coalesce(a.note_body, ''))       LIKE '%whatsapp%'
        OR lower(coalesce(a.communication_body,'')) LIKE '%whatsapp%'
        THEN 'WhatsApp'
      ELSE 'Note'
    END

  -- DEFAULT: communications “source” heuristic
  ELSE
    CASE
      WHEN lower(coalesce(a.communication_source, '')) LIKE '%linkedin%' THEN 'LinkedIn Message'
      WHEN lower(coalesce(a.communication_source, '')) LIKE '%whatsapp%' THEN 'WhatsApp'
      ELSE coalesce(a.activity_type, 'Unknown')
    END
END AS activity_category
  FROM hubspot_datalake.activities a
  JOIN date_bounds db ON TRUE
  JOIN allowed_owners ao ON ao.owner_id = a.owner_id
  WHERE CAST(date_trunc('week', a.occurred_at) AS date) BETWEEN db.first_week_start AND db.last_week_end
    AND (
      a.dt IS NULL OR
      a.dt BETWEEN date_format(db.start_date, '%Y-%m-%d') AND date_format(db.end_date, '%Y-%m-%d')
    )
),
contacts_created AS (
  SELECT
    c.owner_id,
    CAST(date_trunc('week', c.created_at) AS date) AS week_start,
    'Contact created' AS activity_category
  FROM hubspot_datalake.contacts c
  JOIN date_bounds db ON TRUE
  JOIN allowed_owners ao ON ao.owner_id = c.owner_id
  WHERE CAST(date_trunc('week', c.created_at) AS date) BETWEEN db.first_week_start AND db.last_week_end
),
contacts_worked AS (
  SELECT
    c.owner_id,
    CAST(date_trunc('week', c.last_modified_at) AS date) AS week_start,
    'Contact worked' AS activity_category
  FROM hubspot_datalake.contacts c
  JOIN date_bounds db ON TRUE
  JOIN allowed_owners ao ON ao.owner_id = c.owner_id
  WHERE CAST(date_trunc('week', c.last_modified_at) AS date) BETWEEN db.first_week_start AND db.last_week_end
),
deals_created AS (
  SELECT
    d.owner_id,
    CAST(date_trunc('week', d.created_at) AS date) AS week_start,
    'Deal created' AS activity_category
  FROM hubspot_datalake.deals d
  JOIN date_bounds db ON TRUE
  JOIN allowed_owners ao ON ao.owner_id = d.owner_id
  WHERE CAST(date_trunc('week', d.created_at) AS date) BETWEEN db.first_week_start AND db.last_week_end
    AND (
      d.dt IS NULL OR
      d.dt BETWEEN date_format(db.start_date, '%Y-%m-%d') AND date_format(db.end_date, '%Y-%m-%d')
    )
),
unioned AS (
  SELECT owner_id, week_start, activity_category FROM activity_events WHERE activity_category IS NOT NULL
  UNION ALL
  SELECT owner_id, week_start, activity_category FROM contacts_created
  UNION ALL
  SELECT owner_id, week_start, activity_category FROM contacts_worked
  UNION ALL
  SELECT owner_id, week_start, activity_category FROM deals_created
),
final AS (
  SELECT
    u.owner_id,
    COALESCE(o.owner_name, u.owner_id) AS owner_name,
    u.week_start,
    CAST(date_add('day', 6, u.week_start) AS date) AS week_end,
    date_format(u.week_start, '%M ') || CAST(day(u.week_start) AS varchar) ||
      ' to ' ||
      date_format(date_add('day', 6, u.week_start), '%M ') || CAST(day(date_add('day', 6, u.week_start)) AS varchar) ||
      ', ' || CAST(year(date_add('day', 6, u.week_start)) AS varchar) AS week_label_en,
    u.activity_category AS activity_type
  FROM unioned u
  LEFT JOIN hubspot_datalake.owners o ON o.owner_id = u.owner_id
)
SELECT owner_id,
       owner_name,
       week_start,
       week_end,
       week_label_en,
       activity_type,
       COUNT(1) AS activity_count
FROM final
GROUP BY owner_id, owner_name, week_start, week_end, week_label_en, activity_type;