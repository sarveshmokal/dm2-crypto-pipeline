# Real-Time Crypto Market Microstructure Analytics

A real-time data pipeline that ingests high-frequency crypto trade data, computes
two quant-finance microstructure signals -- **Realized Volatility (RV)** and
**Order-Flow Imbalance (OFI)** -- and serves them through a medallion-layered
BigQuery warehouse. Built for *Data Management 2* (SRH Hochschule Heidelberg).

**Research question:** does order-flow imbalance relate to realized volatility and
short-horizon returns across calm vs. turbulent market regimes?

The pipeline follows a **Lambda architecture** (batch layer + speed layer) on top of
a **medallion** data model (bronze -> silver -> gold), running over 8 liquid assets
(BTC, ETH, SOL, BNB, XRP, ADA, DOGE, AVAX).

---

## Architecture

```
                              Binance trade WebSocket (real-time, 8 symbols)
                                          |
                 +------------------------+------------------------+
                 |                                                 |
          BATCH LAYER                                        SPEED LAYER
                 |                                                 |
   ingest_trades_to_gcs.py                            publish_trades_to_pubsub.py
       -> GCS (bronze JSON,                                -> Pub/Sub topic
          partitioned symbol=/dt=)                            "crypto-trades"
                 |                                                 |
   load_trades_to_bronze.py                           dataflow_trades_pipeline.py
       -> bronze.trades_raw (BigQuery)                    (Apache Beam on Dataflow,
                 |                                          1-min event-time windows)
   silver_trades_minute.py (PySpark)                            |
       -> silver.trades_minute                          -> silver.trades_minute_dataflow
                 |                                                 |
                 +------------------------+------------------------+
                                          |
                                   dbt (gold layer)
                          star schema: dim_asset, dim_time,
                          fct_market_window, fct_market_window_stream,
                          fct_ofi_return_by_regime  (+ 17 data-quality tests)
                                          |
                                  BigQuery gold  ->  Looker Studio dashboard

   Orchestration: Apache Airflow DAG (crypto_pipeline) runs the batch path every
   15 minutes. Airflow scheduler and the Pub/Sub publisher run as systemd services
   on a Compute Engine VM. DefiLlama REST API is a second (batch) source feeding
   asset metadata into dim_asset.
```

A second data source, the **DefiLlama REST API**, supplies asset metadata
(reference prices) loaded into `bronze.asset_metadata` and surfaced in `dim_asset`.

---

## The signals (silver/gold computation)

- **Realized Variance / Volatility** -- sum of squared 1-second log returns within
  each minute; `realized_vol = sqrt(realized_variance)`.
- **Bipower Variation** -- jump-robust variance estimator, `(pi/2) * sum(|r_i|*|r_i-1|)`.
- **Jump component** -- `max(realized_variance - bipower_variation, 0)`, isolating
  discontinuous price jumps.
- **Order-Flow Imbalance** -- `(buy_volume - sell_volume) / total_volume`, using
  Binance's `is_buyer_maker` flag, in [-1, +1].
- **Volatility regime** -- trailing z-score of realized vol classifies each minute
  as calm / normal / turbulent (in `fct_ofi_return_by_regime`).

---

## Star schema (gold)

Grain: one row per (asset, minute).

| Table                       | Type            | Notes                                    |
|-----------------------------|-----------------|------------------------------------------|
| `dim_asset`                 | dimension       | 8 assets; name, market_cap_tier, DefiLlama reference_price |
| `dim_time`                  | dimension       | minute grain; hour, day_of_week, is_weekend, trading_session, is_us_market_hours |
| `fct_market_window`         | fact (batch)    | RV, bipower, jump, OFI, OHLC, VWAP        |
| `fct_market_window_stream`  | fact (speed)    | same grain, real-time speed layer         |
| `fct_ofi_return_by_regime`  | fact (analysis) | regime classification + z-scores          |

Foreign keys (`asset_id`, `time_id`) are verified by dbt relationship tests.

---

## Repository layout

```
ingestion/   ingest_trades_to_gcs.py      batch ingest: WebSocket -> GCS bronze
             load_trades_to_bronze.py      GCS -> bronze.trades_raw (per-symbol URI list)
spark/       silver_trades_minute.py       PySpark: bronze -> silver.trades_minute
             publish_trades_to_pubsub.py   speed layer: WebSocket -> Pub/Sub
             dataflow_trades_pipeline.py    Apache Beam on Dataflow -> silver_dataflow
             silver_stream.py               Spark Structured Streaming alternative (documented)
dbt/crypto_dbt/   dbt project: staging -> marts (gold star schema) + tests
airflow/     crypto_pipeline_dag.py         batch orchestration DAG (every 15 min)
deploy/      *.service                      systemd unit files for the VM
requirements-*.txt                          pinned dependencies per environment
```

---

## Reproducing the pipeline

The project uses **three Python virtual environments** on a Compute Engine VM
(Debian 12, Python 3.11), reflecting three independent toolchains:

### 1. Streaming / Spark environment
```bash
python3 -m venv ~/streaming_venv
~/streaming_venv/bin/pip install -r requirements-streaming.txt
```
Runs: `ingest_trades_to_gcs.py`, `load_trades_to_bronze.py`,
`silver_trades_minute.py`, `publish_trades_to_pubsub.py`, `dataflow_trades_pipeline.py`.

### 2. dbt environment
```bash
python3 -m venv ~/dbt_venv
~/dbt_venv/bin/pip install -r requirements-dbt.txt
```
Configure `~/.dbt/profiles.yml` (BigQuery, oauth, dataset=gold, location=EU), then:
```bash
cd dbt/crypto_dbt
~/dbt_venv/bin/dbt run && ~/dbt_venv/bin/dbt test
```

### 3. Airflow environment
```bash
python3 -m venv ~/airflow_venv
~/airflow_venv/bin/pip install -r requirements-airflow.txt   # see file for constraints
export AIRFLOW_HOME=~/airflow
~/airflow_venv/bin/airflow db init
cp airflow/crypto_pipeline_dag.py ~/airflow/dags/
```

### GCP prerequisites
- A GCP project with BigQuery, GCS, Pub/Sub, Dataflow enabled.
- Datasets `bronze`, `silver`, `gold` (location EU).
- A GCS bucket for raw landing.
- A Pub/Sub topic `crypto-trades` + subscription `crypto-trades-sub`.
- VM service account with the cloud-platform scope.

### Automation (systemd, on the VM)
```bash
sudo cp deploy/*.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now crypto-publisher.service        # speed layer ingest
sudo systemctl enable --now crypto-airflow-scheduler.service # batch orchestration
```
The Airflow scheduler then runs the `crypto_pipeline` DAG every 15 minutes.

---

## Cost notes

Designed to run within a student budget (GCP education credit). Cloud Composer
(managed Airflow, ~$300-400/month) was deliberately avoided in favour of a single
small VM running Airflow under systemd. A billing budget with alerts at 50/80/100%
guards spend. Dataflow does not scale to zero, so the streaming job is drained and
the VM stopped when not demonstrating.
