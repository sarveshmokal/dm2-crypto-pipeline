import json
import time
import datetime as dt
import websocket
from google.cloud import storage

PROJECT_ID  = "dm2-crypto-microstructure"
BUCKET_NAME = "dm2-crypto-microstructure-raw"
SYMBOL      = "btcusdt"
# spark watches this prefix; the shim drops a new file here every FLUSH_SECONDS
STREAM_PREFIX = "streaming/trades"
FLUSH_SECONDS = 5

WS_URL = f"wss://data-stream.binance.vision/stream?streams={SYMBOL}@trade"

client = storage.Client(project=PROJECT_ID)
bucket = client.bucket(BUCKET_NAME)

buffer = []
last_flush = time.time()


def flush():
    """write the buffered trades as one file and clear the buffer."""
    global buffer, last_flush
    if not buffer:
        last_flush = time.time()
        return
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S_%f")
    blob_path = f"{STREAM_PREFIX}/trades_{ts}.json"
    payload = "\n".join(json.dumps(r) for r in buffer)
    bucket.blob(blob_path).upload_from_string(payload, content_type="application/json")
    print(f"flushed {len(buffer)} trades -> gs://{BUCKET_NAME}/{blob_path}")
    buffer = []
    last_flush = time.time()


def on_message(ws, message):
    global last_flush
    d = json.loads(message).get("data", {})
    buffer.append({
        "event_type":     d.get("e"),
        "event_time":     d.get("E"),
        "symbol":         d.get("s"),
        "trade_id":       d.get("t"),
        "price":          d.get("p"),
        "quantity":       d.get("q"),
        "trade_time":     d.get("T"),
        "is_buyer_maker": d.get("m"),
    })
    # flush on a time interval so spark sees a steady drip of files
    if time.time() - last_flush >= FLUSH_SECONDS:
        flush()


def on_open(ws):
    print(f"connected, streaming {SYMBOL.upper()} trades to gs://{BUCKET_NAME}/{STREAM_PREFIX}/")


def on_error(ws, error):
    print("error:", error)


def on_close(ws, code, msg):
    print("connection closed, flushing remaining")
    flush()


if __name__ == "__main__":
    # runs continuously until stopped (Ctrl+C); reconnects are handled by re-running
    ws = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws.run_forever(ping_interval=180, ping_timeout=60)

