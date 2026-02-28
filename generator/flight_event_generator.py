"""
Flight Event Generator

Pulls a real snapshot from OpenSky Network API, then simulates a
continuous flight event stream (DEPARTED, EN_ROUTE, LANDED, CANCELLED,
DELAYED) from that baseline data.

Produces events to a Kafka topic: flight-events

Usage:
    python flight_event_generator.py --mode live       # poll OpenSky once, then simulate
    python flight_event_generator.py --mode synthetic  # pure simulation
    python flight_event_generator.py --mode snapshot   # fetch OpenSky and save to JSON
"""

import argparse
import json
import logging
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

import requests
from confluent_kafka import Producer
    
# Logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# Constants

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "flight-events"
OPENSKY_URL = "https://opensky-network.org/api/states/all"

# Realistic airline codes + IATA airport pairs (Emirates region focus)

AIRLINES = ["EK", "FZ", "EY", "QR", "SV", "MS", "RJ", "TK", "BA", "LH"]
AIRPORTS = [
    "DXB", "AUH", "DOH", "KWI", "BAH", "AMM", "CAI", "IST",
    "LHR", "CDG", "FRA", "JFK", "SIN", "BOM", "DEL", "NBO",
]

EVENT_TYPES = ["FLIGHT_DEPARTED", "FLIGHT_EN_ROUTE", "FLIGHT_LANDED",
               "FLIGHT_DELAYED", "FLIGHT_CANCELLED"]

# Event weights — most flights are en-route or landed; fewer cancelled
EVENT_WEIGHTS = [0.15, 0.40, 0.30, 0.12, 0.03]

# DQ intentional anomalies (mirrors real OpenSky gaps) for quality checks
ANOMALY_RATE = 0.05   # 5% of events will have a DQ issue injected


# ── OpenSky Fetch ─────────────────────────────────────────────────────────────
def fetch_opensky_snapshot(username: Optional[str] = None,
                           password: Optional[str] = None) -> list[dict]:
    """Fetch live flight states from OpenSky. Returns list of flight dicts."""
    auth = (username, password) if username and password else None
    try:
        log.info("Fetching snapshot from OpenSky Network API …")
        resp = requests.get(OPENSKY_URL, auth=auth, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        states = data.get("states", []) or []
        flights = []
        for s in states[:500]:          # cap at 500 to stay within rate limits
            if s[1] and s[2]:           # must have callsign + origin country
                flights.append({
                    "callsign": s[1].strip(),
                    "origin_country": s[2],
                    "longitude": s[5],
                    "latitude": s[6],
                    "altitude_m": s[7],
                    "velocity_ms": s[9],
                    "vertical_rate": s[11],
                    "on_ground": s[8],
                })
        log.info(f"OpenSky returned {len(flights)} valid flight states.")
        return flights
    except Exception as exc:
        log.warning(f"OpenSky fetch failed ({exc}). Falling back to synthetic data.")
        return []


# ── Synthetic Flight Builder ──────────────────────────────────────────────────
def build_synthetic_flights(n: int = 200) -> list[dict]:
    """Generate synthetic flight baseline when OpenSky is unavailable."""
    flights = []
    for _ in range(n):
        airline = random.choice(AIRLINES)
        flight_num = f"{airline}{random.randint(100, 9999)}"
        flights.append({
            "callsign": flight_num,
            "origin_country": "UAE",
            "longitude": round(random.uniform(25.0, 60.0), 4),
            "latitude": round(random.uniform(10.0, 50.0), 4),
            "altitude_m": round(random.uniform(0, 12500), 0),
            "velocity_ms": round(random.uniform(0, 280), 1),
            "vertical_rate": round(random.uniform(-15, 15), 2),
            "on_ground": random.random() < 0.1,
        })
    return flights


# ── Event Enricher ────────────────────────────────────────────────────────────
def enrich_to_event(flight: dict) -> dict:
    """
    Convert a raw flight state into a structured flight event.
    Injects realistic DQ anomalies at ANOMALY_RATE for pipeline testing.
    """
    now = datetime.now(timezone.utc)
    airline_code = flight["callsign"][:2] if len(flight["callsign"]) >= 2 else "XX"
    origin = random.choice(AIRPORTS)
    destination = random.choice([a for a in AIRPORTS if a != origin])
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]

    # Scheduled times
    sched_dep_offset = random.randint(-120, 120)   # minutes from now
    sched_dep = now.replace(minute=0, second=0, microsecond=0)
    actual_dep = None
    delay_minutes = 0

    if event_type in ("FLIGHT_DEPARTED", "FLIGHT_EN_ROUTE", "FLIGHT_LANDED"):
        delay_minutes = random.choices(
            [0, random.randint(1, 15), random.randint(16, 60), random.randint(61, 240)],
            weights=[0.60, 0.20, 0.15, 0.05], k=1
        )[0]
        actual_dep = sched_dep.isoformat()

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_timestamp": now.isoformat(),
        "flight_number": flight["callsign"],
        "airline_code": airline_code,
        "origin_airport": origin,
        "destination_airport": destination,
        "scheduled_departure": sched_dep.isoformat(),
        "actual_departure": actual_dep,
        "delay_minutes": delay_minutes,
        "altitude_m": flight.get("altitude_m"),
        "velocity_ms": flight.get("velocity_ms"),
        "latitude": flight.get("latitude"),
        "longitude": flight.get("longitude"),
        "on_ground": flight.get("on_ground", False),
        "origin_country": flight.get("origin_country", "UNKNOWN"),
        "data_source": "opensky_enriched",
        "schema_version": "1.0",
    }

    # ── Inject DQ anomalies (intentional, for pipeline quality checks) ────────
    if random.random() < ANOMALY_RATE:
        anomaly = random.choice([
            "null_airline",
            "null_airport",
            "negative_delay",
            "future_timestamp",
            "duplicate_event_id",
            "invalid_altitude",
        ])
        if anomaly == "null_airline":
            event["airline_code"] = None
        elif anomaly == "null_airport":
            event["origin_airport"] = None
        elif anomaly == "negative_delay":
            event["delay_minutes"] = -random.randint(1, 60)
        elif anomaly == "future_timestamp":
            event["event_timestamp"] = "2099-01-01T00:00:00+00:00"
        elif anomaly == "duplicate_event_id":
            event["event_id"] = "DUPLICATE-TEST-ID-001"
        elif anomaly == "invalid_altitude":
            event["altitude_m"] = random.choice([-500, 99999, None])
        event["_injected_anomaly"] = anomaly   # tag for DQ validation

    return event


# ── Kafka Producer ────────────────────────────────────────────────────────────
def create_producer():
    return Producer({'bootstrap.servers': KAFKA_BOOTSTRAP})


def stream_events(flights: list[dict], interval_sec: float = 1.0,
                  max_events: Optional[int] = None):
    """Continuously produce flight events to Kafka."""
    producer = create_producer()
    count = 0
    log.info(f"Starting event stream → topic: {KAFKA_TOPIC} | interval: {interval_sec}s")
    try:
        while True:
            flight = random.choice(flights)
            event = enrich_to_event(flight)
            key = event["flight_number"]
            producer.produce(KAFKA_TOPIC, key=key, value=json.dumps(event).encode("utf-8"))
            producer.poll(0)
            count += 1

            anomaly_tag = f"  [⚠ {event['_injected_anomaly']}]" if "_injected_anomaly" in event else ""
            log.info(f"[{count}] {event['event_type']:22} | {event['flight_number']:10} | "
                     f"{event['origin_airport']} → {event['destination_airport']}"
                     f"{anomaly_tag}")

            if max_events and count >= max_events:
                log.info(f"Reached max_events={max_events}. Stopping.")
                break

            time.sleep(interval_sec)

    except KeyboardInterrupt:
        log.info("Stream interrupted by user.")
    finally:
        producer.flush()
        log.info(f"Producer closed. Total events sent: {count}")

# ── CLI ───────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Flight Event Generator for Kafka")
    parser.add_argument("--mode", choices=["live", "synthetic", "snapshot"],
                        default="synthetic",
                        help="live=OpenSky+simulate, synthetic=no API, snapshot=fetch+save only")
    parser.add_argument("--username", default=None, help="OpenSky username (optional)")
    parser.add_argument("--password", default=None, help="OpenSky password (optional)")
    parser.add_argument("--interval", type=float, default=1.0,
                        help="Seconds between events (default: 1.0)")
    parser.add_argument("--max-events", type=int, default=None,
                        help="Stop after N events (default: run forever)")
    parser.add_argument("--output", default="opensky_snapshot.json",
                        help="Output file for snapshot mode")
    args = parser.parse_args()

    if args.mode == "snapshot":
        flights = fetch_opensky_snapshot(args.username, args.password)
        with open(args.output, "w") as f:
            json.dump(flights, f, indent=2)
        log.info(f"Snapshot saved to {args.output} ({len(flights)} flights)")
        return

    if args.mode == "live":
        flights = fetch_opensky_snapshot(args.username, args.password)
        if not flights:
            log.warning("No OpenSky data. Switching to synthetic.")
            flights = build_synthetic_flights()
    else:
        flights = build_synthetic_flights()

    stream_events(flights, interval_sec=args.interval, max_events=args.max_events)


if __name__ == "__main__":
    main()
