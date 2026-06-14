import json
import datetime as dt
import websocket
from google.cloud import storage

PROJECT_ID  = "dm2-crypto-microstructure"
BUCKET_NAME = "dm2-crypto-microstructure-raw"
SYMBOL      = "btcusdt"
BATCH_SIZE  = 100
WS_URL      = f"wss://data-stream.binance.vision/stream?streams={SYMBOL}@trade"

trades = []


def on_message(ws, message):
    msg = json.loads(message)
    d = msg.get("data", {})
    # rename binance's single letter keys (bigquery is case-insensitive so e/E and m/M clash)
    rec = {
        "event_type":     d.get("e"),
        "event_time":     d.get("E"),
        "symbol":         d.get("s"),
        "trade_id":       d.get("t"),
        "price":          d.get("p"),
        "quantity":       d.get("q"),
        "trade_time":     d.get("T"),
        "is_buyer_maker": d.get("m"),
    }
    trades.append(rec)
    print(f"collected {len(trades)}/{BATCH_SIZE}  price={rec['price']} qty={rec['quantity']}")
    if len(trades) >= BATCH_SIZE:
        ws.close()


def on_open(ws):
    print(f"connected, collecting {BATCH_SIZE} {SYMBOL.upper()} trades")


def on_error(ws, error):
    print("error:", error)


def upload_to_gcs(records):
    now = dt.datetime.now(dt.timezone.utc)
    date_part = now.strftime("%Y-%m-%d")
    ts = now.strftime("%Y%m%dT%H%M%SZ")
    blob_path = f"bronze/trades/symbol={SYMBOL}/dt={date_part}/trades_{ts}.json"

    payload = "\n".join(json.dumps(r) for r in records)

    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)
    bucket.blob(blob_path).upload_from_string(payload, content_type="application/json")
    print(f"uploaded {len(records)} trades to gs://{BUCKET_NAME}/{blob_path}")


if __name__ == "__main__":
    ws = websocket.WebSocketApp(WS_URL, on_open=on_open, on_message=on_message, on_error=on_error)
    ws.run_forever(ping_interval=180, ping_timeout=60)

    if trades:
        upload_to_gcs(trades)
        print("done")
    else:
        print("no trades collected")
