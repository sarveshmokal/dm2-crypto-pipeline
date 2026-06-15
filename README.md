# Real-Time Crypto Market Microstructure Analytics Pipeline

An end-to-end, automated data pipeline that ingests cryptocurrency market data
**in real time**, refines it through a **medallion architecture**
(bronze → silver → gold) on **Google Cloud Platform**, and exposes
analysis-ready tables for a BI dashboard.

The pipeline runs **two processing paths** — a scheduled **batch** path and a
continuous **streaming** path — that converge in the gold layer. This is the
**Lambda architecture** (a batch/serving layer plus a speed layer) implemented
inside a medallion lakehouse.

**Course:** Data Management 2 — SRH Hochschule Heidelberg
**Author:** Sarvesh Mokal

---

## 1. What this project does

The pipeline studies two quantitative-finance signals derived from
high-frequency market data:

- **Realized Volatility (RV)** — how violently price moves, estimated from
  high-frequency trades using realized variance and the jump-robust
  **bipower variation** estimator.
- **Order-Flow Imbalance (OFI)** — the net buy/sell pressure, derived from
  trade direction.

**Research question:** Does order-flow imbalance relate to realized volatility
and short-horizon returns, and does that relationship differ between calm and
turbulent market regimes?

Crypto is used because it is the only asset class offering **free, real-time,
high-frequency** market data. The methodology transfers directly to equities,
FX, and futures.

---

## 2. Architecture

Two paths, one medallion. The **batch path** is orchestrated by Apache Airflow;
the **streaming path** runs continuously as system services.

```text
                         ┌─────────────────────── BATCH PATH (Airflow DAG, every 15 min) ───────────────────────┐

  DefiLlama REST API ───► ingest_metadata_to_gcs.py ──► GCS (raw JSON) ──► BigQuery bronze ──► PySpark ──► BigQuery silver
  Binance REST (sample) ► ingest_trades_to_gcs.py    ─►                    (load_trades_to_bronze)  (silver_trades_minute)   │
                                                                                                                            │
                         └────────────────────────────────────────────────────────────────────────────────────┘          │
                                                                                                                            ▼
                                                                                                              dbt  ──►  BigQuery GOLD
                                                                                                          (star schema + 17 tests)
                                                                                                                            ▲
                         ┌──────────────────── STREAMING PATH (systemd, continuous) ──────────────────────┐               │
                                                                                                            │               │
  Binance WebSocket ───► stream_trades_to_gcs.py ──► GCS (raw JSON, live) ──► Spark Structured Streaming ──► BigQuery silver┘
   (sub-second trades)    (WS→GCS shim service)        = streaming "bronze"     (silver_stream, watermark)   (trades_minute_stream)

                                                                                                              GOLD ──► BI dashboard
                                                                                                       (Power BI / Looker Studio)
```

**Key architectural decisions**

- **Medallion** (bronze → silver → gold) is the *refinement* axis; **Lambda**
  (batch serving layer + streaming speed layer) is the *latency* axis. They
  coexist: two paths refine through the same medallion layers.
- **ELT, not ETL** — raw data is loaded first, then transformed *in-warehouse*
  with dbt (SQL). BigQuery holds both raw (bronze) and modeled (gold) data.
- **Streaming "bronze" is the GCS lake.** Spark Structured Streaming reads raw
  files directly from GCS (schema-on-read), so the lake *is* the raw layer for
  the streaming path. The batch path additionally materializes a BigQuery bronze
  table because it uses BigQuery's batch-load mechanism.

| Layer         | Technology                                  | Purpose                                                    |
|---------------|---------------------------------------------|------------------------------------------------------------|
| Ingestion     | Python (websocket-client, urllib)           | Live trades from Binance; batch reference from DefiLlama    |
| Raw lake      | Google Cloud Storage                        | Immutable raw JSON (date-partitioned); streaming bronze     |
| Bronze        | BigQuery                                     | Batch raw data loaded as-is, queryable                      |
| Silver        | PySpark (batch) + Spark Structured Streaming | Clean, dedupe, per-minute windows, compute RV + OFI         |
| Gold          | dbt on BigQuery                             | Kimball star schema (2 facts, 2 dimensions) + 17 tests      |
| Orchestration | Apache Airflow (batch) + systemd (streaming) | Automated execution of both paths                          |
| Presentation  | Power BI / Looker Studio                     | Live panels (silver) + analytical panels (gold)             |

---

## 3. Repository structure

```text
dm2-crypto-pipeline/
  ingestion/
    ingest_trades_to_gcs.py      Binance trades -> GCS (batch sample)
    ingest_metadata_to_gcs.py    DefiLlama reference data -> GCS (batch)
    load_trades_to_bronze.py     GCS -> BigQuery bronze (Python client load)
  spark/
    silver_trades_minute.py      BATCH bronze -> silver: full estimators (RV, bipower, jumps, OFI)
    stream_trades_to_gcs.py      STREAMING shim: Binance WebSocket -> GCS (runs as systemd service)
    silver_stream.py             STREAMING bronze(GCS) -> silver: windowed agg + watermark (systemd service)
  dbt/crypto_dbt/
    models/
      staging/
        stg_trades_minute.sql           view over batch silver
        stg_trades_minute_stream.sql    view over streaming silver
      marts/
        fct_market_window.sql           FACT - batch serving layer (asset x minute)
        fct_market_window_stream.sql    FACT - streaming speed layer (asset x minute)
        dim_asset.sql                   DIMENSION - asset attributes
        dim_time.sql                    DIMENSION - time attributes (minute grain)
        fct_ofi_return_by_regime.sql    analysis mart - rolling regime classification
        schema.yml                      17 data-quality + referential-integrity tests
  airflow/
    crypto_pipeline_dag.py       batch DAG: ingest -> bronze -> silver -> gold (every 15 min)
  README.md
```

---

## 4. Data sources

| Source             | Type       | Role                                              | Cadence       |
|--------------------|------------|---------------------------------------------------|---------------|
| **Binance WebSocket** | Real-time | Live trade stream (BTCUSDT) — the speed layer    | Sub-second    |
| **DefiLlama REST**    | Batch     | Asset reference / price metadata — the batch ref | On DAG run    |

Two sources, one real-time — satisfying the requirement. (A third source,
CoinGecko, was scoped as a redundant price cross-check but not built, since two
sources already meet the requirement.)

---

## 5. Data layers in detail

**Bronze (raw).** Trade messages are stored exactly as received. For the batch
path: raw JSON in GCS *and* a BigQuery `bronze` table (loaded via the Python
BigQuery client). For the streaming path: raw JSON in GCS only — the lake is the
raw layer, read directly by Spark (schema-on-read).

**Silver (clean).** Two engines, by path:

- *Batch* (`silver_trades_minute.py`): casts types, converts millisecond
  timestamps, derives trade side from the buyer-maker flag, removes invalid
  rows, aggregates into **per-minute windows**, and computes OHLC, VWAP, volume,
  trade count, **realized variance**, **realized volatility**, **bipower
  variation** (jump-robust), **jump component**, buy/sell volume, signed volume,
  and **OFI**.
- *Streaming* (`silver_stream.py`): Spark Structured Streaming reads raw files
  from GCS, applies a **watermark** for late data, performs the same per-minute
  windowed aggregation, and computes a lighter, lower-latency metric set
  (price-variance proxy + OFI). It writes to a separate silver table.

> **Why two silver tables?** The batch layer computes the full statistical
> estimator suite (bipower/jumps need strict per-trade ordering); the streaming
> layer computes a lighter set for low latency. Merging them into one column
> would put two *different* statistics (true realized variance vs. a proxy) in
> one field, violating dimensional-modeling semantics. Keeping them separate is
> the Lambda batch-vs-speed distinction made explicit.

**Gold (modeled).** dbt transforms silver into a **Kimball star schema**:

- `fct_market_window` — **fact**, batch serving layer, one row per (asset, minute).
- `fct_market_window_stream` — **fact**, streaming speed layer, one row per (asset, minute).
- `dim_asset` — **dimension**, asset attributes.
- `dim_time` — **dimension**, time attributes at minute grain (supports OLAP
  roll-up / drill-down by hour, day, weekday).
- `fct_ofi_return_by_regime` — **analysis mart**: rolling-window regime
  classification (calm / normal / turbulent via trailing z-score) for the
  research question.

Both facts carry foreign keys to both dimensions (`asset_id`, `time_id`).

**Data quality — 17 dbt tests:** uniqueness and not-null on all keys, plus
**referential-integrity** (`relationships`) tests from each fact to each
dimension. All pass.

---

## 6. Automation

Both paths run automatically — no manual steps in normal operation.

**Batch path — Apache Airflow.** The DAG `crypto_pipeline_dag.py` runs four
dependent tasks on a 15-minute schedule:

```text
ingest_trades  >>  load_bronze  >>  spark_silver  >>  dbt_gold (dbt run && dbt test)
```

The final `dbt run` rebuilds the **entire** gold layer — both facts, both
dimensions, the regime mart — and runs all 17 tests.

**Streaming path — systemd services.** Two services on the Compute Engine VM:

- `crypto-shim.service` — runs the WebSocket → GCS shim.
- `crypto-stream.service` — runs the Spark Structured Streaming job.

Both are **enabled** (auto-start on boot) with `Restart=always` (auto-restart on
failure). Verified by rebooting the VM and confirming both services came back up
with no manual intervention.

---

## 7. How to run

**Automated (normal operation).**

```bash
# Batch: the Airflow scheduler runs the DAG every 15 minutes.
airflow scheduler        # (one-off start; thereafter automatic)

# Streaming: the systemd services auto-start on VM boot.
sudo systemctl status crypto-shim.service crypto-stream.service
```

**Manual (for development / one-off runs).**

```bash
# Batch path
python3 ingestion/ingest_trades_to_gcs.py
python3 ingestion/load_trades_to_bronze.py
python3 spark/silver_trades_minute.py
cd dbt/crypto_dbt && dbt run && dbt test

# Streaming path (one-off, foreground)
python3 spark/stream_trades_to_gcs.py &     # shim
python3 spark/silver_stream.py              # streaming job
```

---

## 8. Requirements coverage

| # | Requirement                          | How it is met                                                        |
|---|--------------------------------------|----------------------------------------------------------------------|
| 1 | ≥ 2 data sources, ≥ 1 real-time      | Binance WebSocket (real-time) + DefiLlama REST (batch)               |
| 2 | Extract, clean, load into BigQuery   | Python ingestion + PySpark cleaning + BigQuery bronze/silver layers  |
| 3 | Transform with dbt                   | dbt builds the gold star schema (2 facts, 2 dimensions) + 17 tests   |
| 4 | Include Spark                        | PySpark (batch silver) + Spark Structured Streaming (streaming silver)|
| 5 | Pipeline runs automatically          | Airflow DAG (batch) + systemd services (streaming, self-healing)     |
| 6 | Presentation                         | See presentation deck / `docs/`                                      |

---

## 9. Engineering notes (selected design decisions)

- **Shaded GCS connector** resolves a Guava version conflict between the
  GCS-connector and BigQuery-connector jars in the streaming job (relocated
  Guava classes prevent the clash).
- **Spark `shuffle.partitions` tuned 200 → 4** to match the small data volume
  and the 2-core VM, eliminating task-scheduling overhead that caused streaming
  micro-batches to fall behind the live edge.
- **Environment isolation** via separate Python virtualenvs (Airflow vs.
  streaming) to avoid dependency conflicts.
- **Cost discipline:** plain Compute Engine VM instead of Dataproc/Composer;
  total GCP spend kept well within a small credit budget.

---

## 10. Tech stack

Google Cloud Platform (Cloud Storage, BigQuery, Compute Engine) · Python ·
Apache Spark (PySpark + Structured Streaming) · dbt · Apache Airflow · systemd ·
Power BI / Looker Studio
