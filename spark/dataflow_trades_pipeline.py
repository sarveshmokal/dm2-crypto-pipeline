# Dataflow (Apache Beam) streaming pipeline: read trade messages from Pub/Sub,
# group into 1-minute event-time windows, compute per-minute microstructure
# metrics (realized variance, OFI, OHLC, etc.), and write to BigQuery.
#
# this is the broker-based speed layer: Pub/Sub (transport) -> Dataflow (compute).
# it writes to a SEPARATE table (silver.trades_minute_dataflow) so the existing
# Spark file-based path and its table are untouched. running both demonstrates
# the two standard streaming architectures side by side.

import json
import math
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms import window

PROJECT = "dm2-crypto-microstructure"
SUBSCRIPTION = f"projects/{PROJECT}/subscriptions/crypto-trades-sub"
OUTPUT_TABLE = f"{PROJECT}:silver.trades_minute_dataflow"

OUTPUT_SCHEMA = (
    "symbol:STRING,window_start:TIMESTAMP,open:FLOAT,high:FLOAT,low:FLOAT,"
    "close:FLOAT,volume:FLOAT,trade_count:INTEGER,vwap:FLOAT,"
    "realized_variance:FLOAT,realized_vol:FLOAT,buy_volume:FLOAT,"
    "sell_volume:FLOAT,signed_volume:FLOAT,ofi:FLOAT"
)


def parse_trade(msg_bytes):
    d = json.loads(msg_bytes.decode("utf-8"))
    return {
        "symbol":   d.get("symbol"),
        "trade_id": int(d["trade_id"]) if d.get("trade_id") is not None else 0,
        "price":    float(d["price"]),
        "quantity": float(d["quantity"]),
        "trade_time": int(d["trade_time"]),
        "is_buyer_maker": d.get("is_buyer_maker"),
    }


def with_event_time(trade):
    # assign Beam event-time timestamp from the trade time (ms -> s)
    from apache_beam.transforms.window import TimestampedValue
    return TimestampedValue(trade, trade["trade_time"] / 1000.0)


def key_by_symbol(trade):
    return (trade["symbol"], trade)


def compute_window_metrics(element):
    # element: (symbol, [trades]) for one window
    symbol, trades = element[0], list(element[1])
    # order trades within the window by time then id
    trades.sort(key=lambda t: (t["trade_time"], t["trade_id"]))
    n = len(trades)
    if n == 0:
        return

    prices = [t["price"] for t in trades]
    qtys   = [t["quantity"] for t in trades]
    open_p, close_p = prices[0], prices[-1]
    high_p, low_p = max(prices), min(prices)
    volume = sum(qtys)
    vwap = (sum(p * q for p, q in zip(prices, qtys)) / volume) if volume > 0 else 0.0

    # realized variance = sum of squared log returns between consecutive trades
    rv = 0.0
    for i in range(1, n):
        if prices[i - 1] > 0 and prices[i] > 0:
            r = math.log(prices[i] / prices[i - 1])
            rv += r * r
    rvol = math.sqrt(rv)

    buy_vol  = sum(q for t, q in zip(trades, qtys) if t["is_buyer_maker"] in (False, "false"))
    sell_vol = volume - buy_vol
    signed = buy_vol - sell_vol
    ofi = (signed / volume) if volume > 0 else 0.0

    yield {
        "symbol": symbol,
        "open": open_p, "high": high_p, "low": low_p, "close": close_p,
        "volume": volume, "trade_count": n, "vwap": vwap,
        "realized_variance": rv, "realized_vol": rvol,
        "buy_volume": buy_vol, "sell_volume": sell_vol,
        "signed_volume": signed, "ofi": ofi,
    }


class AddWindowStart(beam.DoFn):
    def process(self, elem, window=beam.DoFn.WindowParam):
        # attach the window start as the minute timestamp (ISO for BigQuery)
        import datetime
        start = window.start.to_utc_datetime().replace(second=0, microsecond=0)
        elem["window_start"] = start.strftime("%Y-%m-%d %H:%M:%S")
        yield elem


def run():
    parser = argparse.ArgumentParser()
    known, pipeline_args = parser.parse_known_args()

    opts = PipelineOptions(pipeline_args, streaming=True, save_main_session=True)

    with beam.Pipeline(options=opts) as p:
        (
            p
            | "ReadPubSub" >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTION)
            | "Parse" >> beam.Map(parse_trade)
            | "EventTime" >> beam.Map(with_event_time)
            | "KeyBySymbol" >> beam.Map(key_by_symbol)
            | "Window1Min" >> beam.WindowInto(window.FixedWindows(60))
            | "GroupByKey" >> beam.GroupByKey()
            | "Metrics" >> beam.ParDo(compute_window_metrics)
            | "AddWindowStart" >> beam.ParDo(AddWindowStart())
            | "WriteBQ" >> beam.io.WriteToBigQuery(
                OUTPUT_TABLE,
                schema=OUTPUT_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


if __name__ == "__main__":
    run()
