# Real-Time Flight Event Pipeline

This is a portfolio project created by Saihan Sadique to demonstrate real-time ingestion using kafka into microsoft fabric lakehouse and makes use of medallion architecture, data quality engineering and CI/CD automation using Github actions.

---

## Architecture

```
OpenSky Network API

Python Event Generator (synthetic flight events + intentional DQ anomalies)

Apache Kafka — topic: flight-events (Docker)

Microsoft Fabric Eventstream (Kafka ingestion)

Fabric Lakehouse — Bronze Delta Table (raw events)

Bronze Notebook — Schema validation + DQ checks → dq_results table

Silver Notebook — Deduplication, cleansing, enrichment

dbt (DuckDB) — Gold models: Airline OTP, Route Performance

GitHub Actions — dbt model + test validation on every push
```

---

## Medallion Architecture

### Bronze
- Raw events landed from Fabric Eventstream
- Schema validation against expected contract
- DQ rules checked and logged to `dq_results` Delta table
- All records kept — flagged with `dq_flag` column
- Partitioned by `event_type`

### Silver
- Deduplication (latest per `event_id` using window function)
- Timestamp parsing, string standardisation (UPPER, TRIM)
- `delay_category` enrichment: ON_TIME / MINOR / MODERATE / SEVERE
- Silver DQ checks logged to `dq_results`
- Partitioned by `event_type`, `delay_category`

### Gold (dbt)
- `gold_airline_otp` — On-Time Performance % with SLA breach flagging per airline
- `gold_route_performance` — avg/max delay, risk flags per route

---


## DQ Anomaly Injection

The Python generator intentionally injects DQ anomalies at a 5% rate to simulate real-world data quality issues:

- `null_airline` — missing airline code
- `null_airport` — missing origin airport
- `negative_delay` — invalid negative delay value
- `future_timestamp` — event timestamp set to 2099
- `duplicate_event_id` — repeated event ID
- `invalid_altitude` — out of range altitude value

Each injected anomaly is tagged with `_injected_anomaly` field for traceability.

---

## Quick Start

### 1. Start Kafka
```
cd kafka
docker-compose up -d
# Kafka UI at http://localhost:8080
```

### 2. Run event generator
```
pip install -r requirements.txt
python generator/flight_event_generator.py --mode synthetic --interval 1.0
```

### 3. Run dbt locally
```
cd flight_pipeline
dbt run
dbt test
```

### 4. Fabric Setup
1. Create Lakehouse: `flight_lakehouse`
2. Create Eventstream, add Custom Endpoint source, connect to Kafka topic
3. Add Lakehouse destination → `bronze_flight_events_raw`
4. Import notebooks from `fabric/notebooks/` — run in order: 01 → 02
5. Create Data Pipeline: Bronze → Silver in sequence

---