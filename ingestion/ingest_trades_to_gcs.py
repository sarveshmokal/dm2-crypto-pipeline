import json
import datetime as dt
from collections import defaultdict
import websocket
from google.cloud import storage

PROJECT_ID  = "dm2-crypto-microstructure"
BUCKET_NAME = "dm2-crypto-microstructure-raw"

# multi-asset: stream trades for several liquid pairs on one websocket connection.
# each trade message already carries its own symbol (the "s" field), so we
# partition the uploaded files by the trade's actual symbol, not a hardcoded one.
SYMBOLS = ["btcusdt", "ethusdt", "solusdt", "bnbusdt", "xrpusdt", "adausdt", "dogeusdt", "avaxusdt"]

# collect a fixed number of trades PER SYMBOL so every asset is represented each run
PER_SYMBOL_TARGET = 60
# hard cap on total messages so a quiet symbol can't block the run forever
MAX_TOTAL = PER_SYMBOL_TARGET * len(SYMBOLS) * 4

_streams = "/".join(f"{s}@trade" for s in SYMBOLS)
WS_URL   = f"wss://data-stream.binance.vision/stream?streams={_streams}"

# trades grouped by symbol
trades_by_symbol = defaultdict(list)
total_seen = 0


def on_message(ws, message):
    global total_seen
    msg = json.loads(message)
    d = msg.get("data", {})
    sym = d.get("s")
    if not sym:
        return
    rec = {
        "event_type":     d.get("e"),
        "event_time":     d.get("E"),
        "symbol":         sym,
        "trade_id":       d.get("t"),
        "price":          d.get("p"),
        "quantity":       d.get("q"),
        "trade_time":     d.get("T"),
        "is_buyer_maker": d.get("m"),
    }
    trades_by_symbol[sym].append(rec)
    total_seen += 1

    enough = (
        len(trades_by_symbol) >= len(SYMBOLS)
        and all(len(trades_by_symbol[s.upper()]) >= PER_SYMBOL_TARGET for s in SYMBOLS)
    )
    if total_seen % 100 == 0:
        have = {k: len(v) for k, v in trades_by_symbol.items()}
        print(f"collected {total_seen} total; per-symbol={have}")
    if enough or total_seen >= MAX_TOTAL:
        ws.close()


def on_open(ws):
    print(f"connected, collecting ~{PER_SYMBOL_TARGET} trades each for {len(SYMBOLS)} symbols")


def on_error(ws, error):
    print("error:", error)


def upload_to_gcs(by_symbol):
    now = dt.datetime.now(dt.timezone.utc)
    date_part = now.strftime("%Y-%m-%d")
    ts = now.strftime("%Y%m%dT%H%M%SZ")
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)
    total = 0
    for sym, records in by_symbol.items():
        if not records:
            continue
        blob_path = f"bronze/trades/symbol={sym}/dt={date_part}/trades_{ts}.json"
        payload = "\n".join(json.dumps(r) for r in records)
        bucket.blob(blob_path).upload_from_string(payload, content_type="application/json")
        total += len(records)
        print(f"uploaded {len(records)} {sym} trades to gs://{BUCKET_NAME}/{blob_path}")
    print(f"uploaded {total} trades across {len(by_symbol)} symbols")


if __name__ == "__main__":
    ws = websocket.WebSocketApp(WS_URL, on_open=on_open, on_message=on_message, on_error=on_error)
    ws.run_forever(ping_interval=180, ping_timeout=60)
    if trades_by_symbol:
        upload_to_gcs(trades_by_symbol)
        print("done")
    else:
        print("no trades collected")
