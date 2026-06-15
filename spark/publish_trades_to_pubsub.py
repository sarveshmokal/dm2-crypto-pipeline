# Publisher: hold the Binance trade WebSocket open and publish each trade as a
# message to a Pub/Sub topic. This is the streaming INGRESS for the Pub/Sub +
# Dataflow path - the broker-based alternative to the file-based (GCS) path.
#
# the original file shim (stream_trades_to_gcs.py) is left untouched so the
# Spark Structured Streaming path keeps working; this is a parallel ingress.

import json
import websocket
from google.cloud import pubsub_v1

PROJECT_ID = "dm2-crypto-microstructure"
TOPIC_ID   = "crypto-trades"
SYMBOL     = "btcusdt"
WS_URL     = f"wss://data-stream.binance.vision/stream?streams={SYMBOL}@trade"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

count = 0


def on_message(ws, message):
    global count
    msg = json.loads(message)
    d = msg.get("data", {})
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
    # publish as UTF-8 JSON bytes; Pub/Sub messages are byte payloads
    publisher.publish(topic_path, json.dumps(rec).encode("utf-8"))
    count += 1
    if count % 50 == 0:
        print(f"published {count} trades to {TOPIC_ID}")


def on_open(ws):
    print(f"connected, publishing {SYMBOL.upper()} trades to Pub/Sub topic {TOPIC_ID}")


def on_error(ws, error):
    print("error:", error)


if __name__ == "__main__":
    ws = websocket.WebSocketApp(WS_URL, on_open=on_open, on_message=on_message, on_error=on_error)
    ws.run_forever(ping_interval=180, ping_timeout=60)
