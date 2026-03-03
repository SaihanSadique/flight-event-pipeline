WITH base AS (
    SELECT * FROM {{ ref('stg_flight_events') }}
    WHERE event_type IN ('FLIGHT_DEPARTED', 'FLIGHT_LANDED')
)

SELECT
    airline_code,
    COUNT(*) AS total_flights,
    SUM(CASE WHEN delay_category = 'ON_TIME' THEN 1 ELSE 0 END) AS on_time_flights,
    SUM(CASE WHEN delay_category = 'SEVERE' THEN 1 ELSE 0 END) AS severely_delayed,
    ROUND(AVG(delay_minutes), 2) AS avg_delay_minutes,
    ROUND(SUM(CASE WHEN delay_category = 'ON_TIME' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS otp_pct,
    CASE WHEN ROUND(SUM(CASE WHEN delay_category = 'ON_TIME' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) < 80 
         THEN TRUE ELSE FALSE END AS sla_breach,
    CURRENT_TIMESTAMP AS computed_at
FROM base
GROUP BY airline_code