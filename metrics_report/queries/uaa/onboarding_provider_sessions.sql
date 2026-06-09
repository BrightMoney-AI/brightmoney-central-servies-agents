WITH session_events AS (
    SELECT
        s.id               AS session_id,
        s.created_at       AS session_created_at,
        COALESCE(
            json_extract_scalar(e.response,    '$.action_data.provider_data.provider'),
            json_extract_scalar(s.session_data,'$.accounts.checking[0].aggregator'),
            json_extract_scalar(s.session_data,'$.session_creation_on_provider_app_response.provider'),
            json_extract_scalar(s.session_data,'$.routing_service_response.provider_name')
        ) AS provider,
        e.event_name
    FROM iceberg_db.brightmoney_core_uaa__public__alsm_accountlinkingsession__current_view_presto s
    JOIN iceberg_db.brightmoney_core_uaa__public__alsm_accountlinkingeventdata__current_view_presto e
      ON e.account_linking_session_id = s.id
    WHERE json_extract_scalar(s.session_data, '$.flow_data.flow_type')    = 'ONBOARDING'
      AND json_extract_scalar(s.session_data, '$.flow_data.linking_for')  = 'CHECKING'
      AND json_extract_scalar(s.session_data, '$.flow_data.linking_flow') = 'ADD'
      AND s.created_at >= CURRENT_DATE - INTERVAL '3' DAY
),
sessions_base AS (
    SELECT
        session_id,
        session_created_at,
        provider,
        MAX(CASE WHEN event_name = 'ACCOUNTS_CREATED_IN_ENTITY_MANAGER_APP_EVENT' THEN 1 ELSE 0 END) AS is_success
    FROM session_events
    GROUP BY session_id, session_created_at, provider
),
d_day AS (
    SELECT
        provider,
        COUNT(*)        AS sessions,
        SUM(is_success) AS success_sessions
    FROM sessions_base
    WHERE DATE(session_created_at) = CURRENT_DATE - INTERVAL '1' DAY
      AND provider IN ('AKOYA', 'PLAID', 'DL_CAPITALONE')
    GROUP BY provider
),
d_minus_1 AS (
    SELECT
        provider,
        COUNT(*)        AS sessions,
        SUM(is_success) AS success_sessions
    FROM sessions_base
    WHERE DATE(session_created_at) = CURRENT_DATE - INTERVAL '2' DAY
      AND provider IN ('AKOYA', 'PLAID', 'DL_CAPITALONE')
    GROUP BY provider
)
SELECT
    COALESCE(d.provider,          d1.provider) AS provider,
    COALESCE(d.sessions,          0)           AS d_sessions,
    COALESCE(d.success_sessions,  0)           AS d_success,
    COALESCE(d1.sessions,         0)           AS d1_sessions,
    COALESCE(d1.success_sessions, 0)           AS d1_success
FROM d_day d
FULL OUTER JOIN d_minus_1 d1 ON d1.provider = d.provider
ORDER BY COALESCE(d.provider, d1.provider)
