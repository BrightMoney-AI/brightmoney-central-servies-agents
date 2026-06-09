WITH successful_sessions AS (
    SELECT DISTINCT
        s.id         AS session_id,
        s.created_at,
        JSON_EXTRACT_SCALAR(s.session_data, '$.flow_data.client_source') AS client_source,
        CASE
            WHEN JSON_EXTRACT_SCALAR(s.session_data, '$.flow_data.flow_type') = 'ONBOARDING'
            THEN 'Onboarding'
            ELSE 'Other'
        END AS flow_type
    FROM iceberg_db.brightmoney_core_uaa__public__alsm_accountlinkingsession__current_view_presto s
    JOIN iceberg_db.brightmoney_core_uaa__public__alsm_accountlinkingeventdata__current_view_presto e
        ON e.account_linking_session_id = s.id
    WHERE e.event_name = 'ACCOUNTS_CREATED_IN_ENTITY_MANAGER_APP_EVENT'
      AND JSON_EXTRACT_SCALAR(s.session_data, '$.flow_data.client_source') IN ('web', 'android', 'ios')
      AND s.created_at >= CURRENT_DATE - INTERVAL '2' DAY
),
yesterday_agg AS (
    SELECT client_source, flow_type, COUNT(*) AS sessions
    FROM successful_sessions
    WHERE DATE(created_at) = CURRENT_DATE - INTERVAL '1' DAY
    GROUP BY client_source, flow_type
),
day_before_agg AS (
    SELECT client_source, flow_type, COUNT(*) AS sessions
    FROM successful_sessions
    WHERE DATE(created_at) = CURRENT_DATE - INTERVAL '2' DAY
    GROUP BY client_source, flow_type
)
SELECT
    COALESCE(y.client_source, d.client_source) AS client_source,
    COALESCE(y.flow_type,     d.flow_type)     AS flow_type,
    COALESCE(y.sessions,      0)               AS yesterday_sessions,
    COALESCE(d.sessions,      0)               AS day_before_sessions
FROM yesterday_agg y
FULL OUTER JOIN day_before_agg d
    ON  d.client_source = y.client_source
    AND d.flow_type     = y.flow_type
ORDER BY client_source, flow_type
