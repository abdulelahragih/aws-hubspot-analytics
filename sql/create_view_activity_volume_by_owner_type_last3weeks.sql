CREATE
OR REPLACE VIEW "activity_volume_by_owner_type_last3weeks_v3" AS WITH week_bounds AS (
		SELECT (
				CASE
					WHEN day_of_week(
						CAST(
							current_timestamp AT TIME ZONE 'America/Montevideo' AS date
						)
					) = 7 THEN CAST(
						current_timestamp AT TIME ZONE 'America/Montevideo' AS date
					) ELSE date_add(
						'day',
						- day_of_week(
							CAST(
								current_timestamp AT TIME ZONE 'America/Montevideo' AS date
							)
						),
						CAST(
							current_timestamp AT TIME ZONE 'America/Montevideo' AS date
						)
					)
				END
			) last_sunday
	),
	last_3_weeks AS (
		SELECT CAST(date_add('day', -6, last_sunday) AS date) week1_start,
			CAST(last_sunday AS date) week1_end,
			CAST(date_add('day', -13, last_sunday) AS date) week2_start,
			CAST(date_add('day', -7, last_sunday) AS date) week2_end,
			CAST(date_add('day', -20, last_sunday) AS date) week3_start,
			CAST(date_add('day', -14, last_sunday) AS date) week3_end
		FROM week_bounds
	),
	date_bounds AS (
		SELECT week3_start first_week_start,
			week1_end last_week_end,
			week3_start start_date,
			week1_end end_date
		FROM last_3_weeks
	),
	allowed_owners AS (
		SELECT '47715895' owner_id
		UNION ALL
		SELECT '54166505'
		UNION ALL
		SELECT '81463777'
		UNION ALL
		SELECT '638599207'
		UNION ALL
		SELECT '1271887343'
	),
	activity_events AS (
		SELECT a.owner_id,
			CAST(
				date_trunc(
					'week',
					at_timezone(a.created_at, 'America/Montevideo')
				) AS date
			) week_start,
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
		FROM (
				(
					hubspot_datalake.activities a
					INNER JOIN date_bounds db ON true
				)
				INNER JOIN allowed_owners ao ON (ao.owner_id = a.owner_id)
			)
		WHERE (
				CAST(
					date_trunc(
						'week',
						at_timezone(a.created_at, 'America/Montevideo')
					) AS date
				) BETWEEN db.first_week_start AND db.last_week_end
				AND (
					(a.dt IS NULL)
					OR (
						a.dt BETWEEN date_format(db.start_date, '%Y-%m-%d') AND date_format(db.end_date, '%Y-%m-%d')
					)
				)
			)
	),
	contacts_created AS (
		SELECT c.owner_id,
			CAST(
				date_trunc(
					'week',
					at_timezone(c.created_at, 'America/Montevideo')
				) AS date
			) week_start,
			'Contact created' activity_category
		FROM (
				(
					hubspot_datalake.contacts_75c422d9e8feb7b640a834d7f98a57f8 c
					INNER JOIN date_bounds db ON true
				)
				INNER JOIN allowed_owners ao ON (ao.owner_id = c.owner_id)
			)
		WHERE (
				CAST(
					date_trunc(
						'week',
						at_timezone(c.created_at, 'America/Montevideo')
					) AS date
				) BETWEEN db.first_week_start AND db.last_week_end
			)
	),
	contacts_worked AS (
		SELECT c.owner_id,
			CAST(
				date_trunc(
					'week',
					at_timezone(c.last_modified_at, 'America/Montevideo')
				) AS date
			) week_start,
			'Contact worked' activity_category
		FROM (
				(
					hubspot_datalake.contacts_75c422d9e8feb7b640a834d7f98a57f8 c
					INNER JOIN date_bounds db ON true
				)
				INNER JOIN allowed_owners ao ON (ao.owner_id = c.owner_id)
			)
		WHERE (
				CAST(
					date_trunc(
						'week',
						at_timezone(c.last_modified_at, 'America/Montevideo')
					) AS date
				) BETWEEN db.first_week_start AND db.last_week_end
			)
	),
	deals_created AS (
		SELECT d.owner_id,
			CAST(
				date_trunc(
					'week',
					at_timezone(d.created_at, 'America/Montevideo')
				) AS date
			) week_start,
			'Deal created' activity_category
		FROM (
				(
					hubspot_datalake.deals_latest d
					INNER JOIN date_bounds db ON true
				)
				INNER JOIN allowed_owners ao ON (ao.owner_id = d.owner_id)
			)
		WHERE (
				CAST(
					date_trunc(
						'week',
						at_timezone(d.created_at, 'America/Montevideo')
					) AS date
				) BETWEEN db.first_week_start AND db.last_week_end
				AND (
					(d.dt IS NULL)
					OR (
						d.dt BETWEEN date_format(db.start_date, '%Y-%m-%d') AND date_format(db.end_date, '%Y-%m-%d')
					)
				)
			)
	),
	unioned AS (
		SELECT owner_id,
			week_start,
			activity_category
		FROM activity_events
		WHERE (activity_category IS NOT NULL)
		UNION ALL
		SELECT owner_id,
			week_start,
			activity_category
		FROM contacts_created
		UNION ALL
		SELECT owner_id,
			week_start,
			activity_category
		FROM contacts_worked
		UNION ALL
		SELECT owner_id,
			week_start,
			activity_category
		FROM deals_created
	),
	final AS (
		SELECT u.owner_id,
			COALESCE(o.owner_name, u.owner_id) owner_name,
			u.week_start,
			CAST(date_add('day', 6, u.week_start) AS date) week_end,
			concat(
				concat(
					concat(
						concat(
							concat(
								concat(
									date_format(u.week_start, '%M '),
									CAST(day(u.week_start) AS varchar)
								),
								' to '
							),
							date_format(date_add('day', 6, u.week_start), '%M ')
						),
						CAST(day(date_add('day', 6, u.week_start)) AS varchar)
					),
					', '
				),
				CAST(
					year(date_add('day', 6, u.week_start)) AS varchar
				)
			) week_label_en,
			u.activity_category activity_type
		FROM (
				unioned u
				LEFT JOIN hubspot_datalake.owners o ON (o.owner_id = u.owner_id)
			)
	)
SELECT owner_id,
       owner_name,
       week_start,
       week_end,
       week_label_en,
       activity_type,
       COUNT(1) activity_count
FROM final
GROUP BY owner_id,
         owner_name,
         week_start,
         week_end,
         week_label_en,
         activity_type;