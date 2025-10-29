"""
Kafka Producer (Public-safe)

- No hardcoded IPs or secrets.
- Reads config from environment variables or CLI args.
- Gentle rate limiting + timeouts + retries.
- Optional idempotency_id per message.

Usage (local example):
    export KAFKA_BROKERS="localhost:9092"
    export KAFKA_TOPIC="datalakex"
    python kafka/producer.py --count 50 --sleep 1

Requirements:
    pip install kafka-python requests
"""

from __future__ import annotations
import os
import json
import time
import uuid
import argparse
from typing import Dict, Any

import requests
from requests.adapters import HTTPAdapter, Retry
from kafka import KafkaProducer


def make_http_session(timeout_sec: float = 5.0) -> requests.Session:
    """Build a resilient requests Session with retries/timeouts."""
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.5,  # 0.5, 1, 2, 4, ...
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST"])
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({"User-Agent": "kafka-producer-demo/1.0"})
    session.request_timeout = timeout_sec  # just for reference
    return session


def fetch_random_employee(session: requests.Session, timeout_sec: float = 5.0) -> Dict[str, Any] | None:
    """Fetch a random user and flatten minimal fields."""
    resp = session.get("https://randomuser.me/api/", timeout=timeout_sec)
    if resp.status_code != 200:
        return None
    user = resp.json()["results"][0]
    return {
        "name": f"{user['name']['first']} {user['name']['last']}",
        "gender": user["gender"],
        "age": user["dob"]["age"],
        "country": user["location"]["country"],
        "email": user["email"],
    }


def build_producer(bootstrap_servers: str) -> KafkaProducer:
    """Create Kafka producer with UTF-8 JSON serializer."""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        linger_ms=10,
        acks="all",
        retries=3,
    )


def main():
    parser = argparse.ArgumentParser(description="Public-safe Kafka Producer")
    parser.add_argument("--brokers", default=os.getenv("KAFKA_BROKERS", "localhost:9092"),
                        help="Kafka bootstrap servers, e.g. localhost:9092")
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "datalakex"),
                        help="Kafka topic name")
    parser.add_argument("--count", type=int, default=int(os.getenv("MSG_COUNT", "50")),
                        help="Number of messages to produce")
    parser.add_argument("--sleep", type=float, default=float(os.getenv("MSG_SLEEP_SEC", "1")),
                        help="Seconds to sleep between messages")
    parser.add_argument("--timeout", type=float, default=float(os.getenv("HTTP_TIMEOUT", "5")),
                        help="HTTP timeout seconds")
    parser.add_argument("--idempotency", action="store_true", default=os.getenv("ADD_IDEMPOTENCY", "0") == "1",
                        help="Add idempotency_id to each message")
    args = parser.parse_args()

    session = make_http_session(timeout_sec=args.timeout)
    producer = build_producer(args.brokers)

    sent = 0
    try:
        for _ in range(args.count):
            try:
                payload = fetch_random_employee(session, timeout_sec=args.timeout)
                if not payload:
                    print("Skip: failed to fetch random user")
                    time.sleep(args.sleep)
                    continue

                if args.idempotency:
                    payload["idempotency_id"] = str(uuid.uuid4())

                print(f"Producing → {payload}")
                producer.send(args.topic, value=payload)
                sent += 1
                time.sleep(args.sleep)
            except KeyboardInterrupt:
                print("\nInterrupted by user.")
                break
            except Exception as e:
                print(f"Error producing message: {e}")
                time.sleep(args.sleep)

        producer.flush()
    finally:
        producer.close()

    print(f"Done. Sent {sent} message(s) to topic '{args.topic}'.")
    

if __name__ == "__main__":
    main()
