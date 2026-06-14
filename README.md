# Real-Time Crypto Market Microstructure Analytics Pipeline

An end-to-end, automated data pipeline that ingests real-time cryptocurrency
market data, refines it through a **medallion architecture** (bronze -> silver ->
gold) on **Google Cloud Platform**, and exposes analysis-ready tables for a
BI dashboard.

**Course:** Data Management 2 — SRH Hochschule Heidelberg
**Author:** Sarvesh Mokal

---

## 1. What this project does

The pipeline studies two quantitative-finance signals derived from
high-frequency market data:

- **Realized Volatility (RV)** — how violently price moves, estimated from
  high-frequency trades.
- **Order-Flow Imbalance (OFI)** — the net buy/sell pressure, derived from
  trade direction.

**Research question:** Does order-flow imbalance relate to realized volatility
and short-horizon returns, and does that relationship differ between calm and
turbulent market regimes?

Crypto is used because it is the only asset class offering **free, real-time,
high-frequency** market data. The methodology transfers directly to equities,
FX, and futures.

---

## 2. Architecture (Medallion on GCP)

```text
Binance WebSocket (real-time)  ->  Python ingestion  ->  GCS raw lake (JSON files)
                                                         + BigQuery bronze (raw rows)
                                                                 |
                                                                 v
DefiLlama / CoinGecko (batch)  ->  scheduled pull    ->  PySpark  ->  BigQuery silver
                                                         (clean, dedupe, per-minute,
                                                          compute RV + OFI)
                                                                 |
                                                                 v
                                                         dbt  ->  BigQuery gold
                                                         (Kimball star schema + tests)
                                                                 |
                                                                 v
                                                         Power BI dashboard

Orchestration: Apache Airflow runs the steps automatically, in dependency order.
```

| Layer         | Technology                | Purpose                                               |
|---------------|---------------------------|-------------------------------------------------------|
| Ingestion     | Python (websocket-client) | Pull live trades from Binance, land raw data          |
| Raw lake      | Google Cloud Storage      | Immutable raw JSON archive (date-partitioned)         |
| Bronze        | BigQuery                  | Raw data loaded as-is, queryable                      |
| Silver        | PySpark (Dataproc)        | Clean, dedupe, per-minute windows, compute metrics    |
| Gold          | dbt (BigQuery)            | Kimball star schema + data-quality tests              |
| Orchestration | Apache Airflow            | Runs the pipeline automatically (dependency order)    |

---

## 3. Repository structure

```text
dm2-crypto-pipeline/
  ingestion/
    ingest_trades_to_gcs.py     Binance WebSocket -> GCS + BigQuery bronze
  spark/
    silver_trades_minute.py     Bronze -> Silver: clean + per-minute metrics
  dbt/
    crypto_dbt/                 Silver -> Gold: star schema + tests
      models/
        staging/stg_trades_minute.sql
        marts/fct_market_window.sql    FACT table (asset x minute)
        marts/dim_asset.sql            DIMENSION table
        marts/schema.yml               data-quality + referential tests
  airflow/                      orchestration DAGs (automation)
  docs/                         architecture diagram, design document
  README.md
```

---

## 4. Data layers in detail

**Bronze (raw):** Trade messages from Binance are stored exactly as received —
as JSON files in GCS (the immutable archive) and as rows in a BigQuery table
(queryable). No cleaning happens here; bronze is the faithful record.

**Silver (clean):** PySpark reads bronze, casts types, converts millisecond
timestamps, derives trade side (BUY/SELL) from the buyer-maker flag, removes
invalid rows, and aggregates trades into **per-minute windows**. For each
window it computes OHLC, VWAP, volume, trade count, realized volatility,
buy/sell volume, and signed volume (order-flow imbalance).

**Gold (modeled):** dbt transforms silver into a **Kimball star schema**:

- `fct_market_window` — the fact table, one row per (asset, minute).
- `dim_asset` — the asset dimension (descriptive attributes).
- Data-quality tests: uniqueness, not-null, and **referential integrity**
  between the fact and dimension.

---

## 5. How to run (current manual steps)

```bash
# 1. Ingest live trades -> GCS + BigQuery bronze
python3 ingestion/ingest_trades_to_gcs.py
bq load --source_format=NEWLINE_DELIMITED_JSON --autodetect \
  dm2-crypto-microstructure:bronze.trades_raw \
  "gs://dm2-crypto-microstructure-raw/bronze/trades/symbol=*/dt=*/trades_*.json"

# 2. Bronze -> Silver (PySpark)
python3 spark/silver_trades_minute.py

# 3. Silver -> Gold (dbt)
cd dbt/crypto_dbt
dbt run     # build staging + gold models
dbt test    # run data-quality + referential-integrity tests
```

*(Automation via Apache Airflow replaces these manual steps — see `airflow/`.)*

---

## 6. Requirements coverage

| # | Requirement                       | How it is met                                               |
|---|-----------------------------------|-------------------------------------------------------------|
| 1 | >= 2 data sources, >= 1 real-time | Binance WebSocket (real-time) + DefiLlama/CoinGecko (batch)  |
| 2 | Extract, clean, load into BigQuery| Ingestion + PySpark cleaning + BigQuery layers              |
| 3 | Transform with dbt                | dbt builds the gold star schema                             |
| 4 | Include Spark                     | PySpark performs bronze -> silver                           |
| 5 | Pipeline runs automatically       | Apache Airflow orchestration                                |
| 6 | Presentation                      | See `docs/`                                                 |

---

## 7. Tech stack

Google Cloud Platform (Cloud Storage, BigQuery, Dataproc) · Python · Apache
Spark (PySpark) · dbt · Apache Airflow · Power BI
