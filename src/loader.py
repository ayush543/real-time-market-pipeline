import json
from pathlib import Path
from datetime import datetime

from db import get_connection


def load_latest_raw_file():
    raw_dir = Path("data/raw")
    json_files = sorted(raw_dir.glob("market_*.json"))

    if not json_files:
        raise FileNotFoundError("No raw JSON files found in data/raw/")

    latest_file = json_files[-1]
    print(f"Loading file: {latest_file}")

    with open(latest_file, "r") as f:
        data = json.load(f)

    return data


def get_previous_price(cur, asset_id):
    query = """
        SELECT price_usd
        FROM raw_market_data
        WHERE asset_id = %s
        ORDER BY event_ts DESC
        LIMIT 1
    """
    cur.execute(query, (asset_id,))
    result = cur.fetchone()

    if result is None:
        return None

    return float(result[0])


def is_price_anomaly(current_price, previous_price, threshold=5.0):
    if previous_price is None:
        return False

    if previous_price == 0:
        return False

    pct_change = abs((current_price - previous_price) / previous_price) * 100
    return pct_change > threshold


def insert_records(records):
    conn = get_connection()
    cur = conn.cursor()

    insert_query = """
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
            is_anomaly,
            raw_payload
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    for record in records:
        asset_id = record.get("id")
        current_price = float(record.get("current_price"))

        previous_price = get_previous_price(cur, asset_id)
        anomaly_flag = is_price_anomaly(current_price, previous_price, threshold=5.0)

        source_last_updated = None
        if record.get("last_updated"):
            source_last_updated = datetime.fromisoformat(
                record["last_updated"].replace("Z", "+00:00")
            )

        values = (
            asset_id,
            record.get("symbol"),
            record.get("name"),
            current_price,
            record.get("market_cap"),
            record.get("total_volume"),
            record.get("price_change_percentage_24h"),
            source_last_updated,
            datetime.now(),
            anomaly_flag,
            json.dumps(record),
        )

        print(
            f"Asset: {asset_id} | Current price: {current_price} | "
            f"Previous price: {previous_price} | Anomaly: {anomaly_flag}"
        )

        cur.execute(insert_query, values)

    conn.commit()
    cur.close()
    conn.close()

    print(f"Inserted {len(records)} records into raw_market_data")


if __name__ == "__main__":
    records = load_latest_raw_file()
    insert_records(records)