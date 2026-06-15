# Publisher: hold the Binance trade WebSocket open and publish each trade as a
# message to a Pub/Sub topic. Streaming INGRESS for the Pub/Sub + Dataflow path.
#
# multi-asset: subscribes to several liquid pairs on one websocket. each trade
# message carries its own symbol, which the Dataflow pipeline keys on, so no
# downstream change is needed to support many assets.

import json
import websocket
from google.cloud import pubsub_v1

PROJECT_ID = "dm2-crypto-microstructure"
TOPIC_ID   = "crypto-trades"
SYMBOLS    = ["btcusdt", "ethusdt", "solusdt", "bnbusdt", "xrpusdt", "adausdt", "dogeusdt", "avaxusdt"]

_streams = "/".join(f"{s}@trade" for s in SYMBOLS)
WS_URL   = f"wss://data-stream.binance.vision/stream?streams={_streams}"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

count = 0


def on_message(ws, message):
    global count
    msg = json.loads(message)
    d = msg.get("data", {})
    if not d.get("s"):
        return
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
    publisher.publish(topic_path, json.dumps(rec).encode("utf-8"))
    count += 1
    if count % 100 == 0:
        print(f"published {count} trades to {TOPIC_ID}")


def on_open(ws):
    print(f"connected, publishing {len(SYMBOLS)} symbols to Pub/Sub topic {TOPIC_ID}")


def on_error(ws, error):
    print("error:", error)


if __name__ == "__main__":
    ws = websocket.WebSocketApp(WS_URL, on_open=on_open, on_message=on_message, on_error=on_error)
    ws.run_forever(ping_interval=180, ping_timeout=60)
