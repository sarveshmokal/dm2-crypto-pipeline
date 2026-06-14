import json
import datetime as dt
import urllib.request
from google.cloud import storage

PROJECT_ID  = "dm2-crypto-microstructure"
BUCKET_NAME = "dm2-crypto-microstructure-raw"

# defillama batch source - current price/symbol per coin
COINS = ["coingecko:bitcoin", "coingecko:ethereum", "coingecko:solana"]
API_URL = "https://coins.llama.fi/prices/current/" + ",".join(COINS)


def fetch_metadata():
    with urllib.request.urlopen(API_URL, timeout=30) as resp:
        data = json.loads(resp.read())

    rows = []
    # flatten the nested {"coins": {id: {...}}} into one record per coin
    for coin_id, info in data.get("coins", {}).items():
        rows.append({
            "coin_id":    coin_id,                 # e.g. coingecko:bitcoin
            "symbol":     info.get("symbol"),      # BTC / ETH / SOL
            "price":      info.get("price"),
            "timestamp":  info.get("timestamp"),
            "confidence": info.get("confidence"),
            "source":     "defillama",
        })
    return rows


def upload_to_gcs(records):
    now = dt.datetime.now(dt.timezone.utc)
    date_part = now.strftime("%Y-%m-%d")
    ts = now.strftime("%Y%m%dT%H%M%SZ")
    blob_path = f"bronze/asset_metadata/dt={date_part}/metadata_{ts}.json"

    payload = "\n".join(json.dumps(r) for r in records)

    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)
    bucket.blob(blob_path).upload_from_string(payload, content_type="application/json")
    print(f"uploaded {len(records)} metadata rows to gs://{BUCKET_NAME}/{blob_path}")


if __name__ == "__main__":
    rows = fetch_metadata()
    for r in rows:
        print(f"{r['symbol']}: price={r['price']} confidence={r['confidence']}")
    upload_to_gcs(rows)
    print("done")
