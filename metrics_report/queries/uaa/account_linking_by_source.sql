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
      AND s.created_at >= NOW() - INTERVAL '28' HOUR
),
today_agg AS (
    SELECT client_source, flow_type, COUNT(*) AS sessions
    FROM successful_sessions
    WHERE created_at >= NOW() - INTERVAL '4' HOUR
    GROUP BY client_source, flow_type
),
yesterday_agg AS (
    SELECT client_source, flow_type, COUNT(*) AS sessions
    FROM successful_sessions
    WHERE created_at >= NOW() - INTERVAL '28' HOUR
      AND created_at <  NOW() - INTERVAL '24' HOUR
    GROUP BY client_source, flow_type
)
SELECT
    COALESCE(t.client_source, y.client_source) AS client_source,
    COALESCE(t.flow_type,     y.flow_type)     AS flow_type,
    COALESCE(t.sessions,      0)               AS today_sessions,
    COALESCE(y.sessions,      0)               AS yesterday_sessions
FROM today_agg t
FULL OUTER JOIN yesterday_agg y
    ON  y.client_source = t.client_source
    AND y.flow_type     = t.flow_type
ORDER BY client_source, flow_type
