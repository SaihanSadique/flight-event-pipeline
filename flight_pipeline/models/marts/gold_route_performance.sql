WITH base AS (
    SELECT * FROM {{ ref('stg_flight_events') }}
    WHERE origin_airport IS NOT NULL 
    AND destination_airport IS NOT NULL
)
SELECT
    origin_airport,
    destination_airport,
    origin_airport || '->' || destination_airport AS route,
    COUNT(*) AS total_flights,
    ROUND(AVG(delay_minutes), 2) AS avg_delay_minutes,
    MAX(delay_minutes) AS max_delay_minutes,
    SUM(CASE WHEN delay_category = 'SEVERE' THEN 1 ELSE 0 END) AS severe_delays,
    CASE WHEN AVG(delay_minutes) > 30 THEN TRUE ELSE FALSE END AS risk_flag,
    CURRENT_TIMESTAMP AS computed_at
FROM base
GROUP BY origin_airport, destination_airport