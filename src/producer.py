import json
import time
from datetime import datetime, timezone
import requests
from confluent_kafka import Producer
from src.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

API_URL = "https://api.coingecko.com/api/v3/coins/markets"
ASSETS = ["bitcoin", "ethereum", "solana"]


def delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] Delivery failed: {err}")
    else:
        print(f"[INFO] Sent to {msg.topic()} [{msg.partition()}] offset={msg.offset()}")


def get_market_data():
    params = {
        "vs_currency": "usd",
        "ids": ",".join(ASSETS),
        "order": "market_cap_desc",
        "per_page": len(ASSETS),
        "page": 1,
        "sparkline": "false",
        "price_change_percentage": "24h",
    }
    response = requests.get(API_URL, params=params, timeout=15)
    response.raise_for_status()
    return response.json()


def transform_record(record):
    return {
        "asset_id": record["id"],
        "symbol": record["symbol"],
        "name": record["name"],
        "price_usd": record["current_price"],
        "market_cap": record.get("market_cap"),
        "total_volume": record.get("total_volume"),
        "price_change_pct_24h": record.get("price_change_percentage_24h"),
        "source_last_updated": record.get("last_updated"),
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "raw_payload": record,
    }


def main():
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "market-producer"
    })

    print(f"[INFO] Producing to topic: {KAFKA_TOPIC}")

    while True:
        try:
            data = get_market_data()

            for record in data:
                payload = transform_record(record)
                producer.produce(
                    topic=KAFKA_TOPIC,
                    key=payload["asset_id"],
                    value=json.dumps(payload),
                    callback=delivery_report,
                )

            producer.poll(0)
            producer.flush()
            print("[INFO] Batch sent to Kafka")

        except Exception as e:
            print(f"[ERROR] Producer failed: {e}")

        time.sleep(20)


if __name__ == "__main__":
    main()
