WITH source AS (
    SELECT * FROM read_csv_auto('C:/Users/saiha/OneDrive/Documents/Projects/flight-event-pipeline/data/silver_flight_events.csv')
)

SELECT
    event_id,
    event_type,
    event_timestamp,
    flight_number,
    airline_code,
    origin_airport,
    destination_airport,
    delay_minutes,
    delay_category,
    is_long_haul,
    processed_at
FROM source