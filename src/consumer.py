import json
import psycopg2
from confluent_kafka import Consumer, KafkaException
from src.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)


def get_db_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def insert_market_record(conn, record):
    query = """
        INSERT INTO raw_market_data (
            asset_id,
            symbol,
            name,
            price_usd,
            market_cap,
            total_volume,
            price_change_pct_24h,
            source_last_updated,
            event_ts,
            raw_payload
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
    """

    values = (
        record["asset_id"],
        record["symbol"],
        record["name"],
        record["price_usd"],
        record.get("market_cap"),
        record.get("total_volume"),
        record.get("price_change_pct_24h"),
        record.get("source_last_updated"),
        record["event_ts"],
        json.dumps(record["raw_payload"]),
    )

    with conn.cursor() as cur:
        cur.execute(query, values)
    conn.commit()


def main():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "market-db-writer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })

    consumer.subscribe([KAFKA_TOPIC])
    conn = get_db_connection()

    print(f"[INFO] Consuming from topic: {KAFKA_TOPIC}")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                raise KafkaException(msg.error())

            try:
                record = json.loads(msg.value().decode("utf-8"))
                insert_market_record(conn, record)
                print(f"[INFO] Inserted {record['asset_id']}")
            except Exception as e:
                print(f"[ERROR] Failed processing message: {e}")

    except KeyboardInterrupt:
        print("[INFO] Consumer stopped")

    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
