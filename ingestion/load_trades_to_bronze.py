import datetime as dt
from google.cloud import bigquery

PROJECT = "dm2-crypto-microstructure"
BUCKET  = "dm2-crypto-microstructure-raw"
TABLE   = f"{PROJECT}.bronze.trades_raw"

# multi-asset: load every symbol partition for today. BigQuery does NOT support a
# wildcard in the middle of a URI (e.g. symbol=*/dt=...), so we build one explicit
# URI per symbol and pass the list to load_table_from_uri (which accepts a list).
SYMBOLS = ["btcusdt", "ethusdt", "solusdt", "bnbusdt", "xrpusdt", "adausdt", "dogeusdt", "avaxusdt"]

# today's partition, matches where the ingestion script wrote (folders are UPPERCASE)
today = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
uris = [
    f"gs://{BUCKET}/bronze/trades/symbol={s.upper()}/dt={today}/trades_*.json"
    for s in SYMBOLS
]

client = bigquery.Client(project=PROJECT)
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    autodetect=True,
    write_disposition="WRITE_TRUNCATE",   # replace, same as --replace
    ignore_unknown_values=True,
)
print(f"loading {len(uris)} symbol partitions into {TABLE}")
# a symbol with no file today would 404 the whole load, so only keep URIs that exist
from google.cloud import storage
gcs = storage.Client(project=PROJECT)
bucket = gcs.bucket(BUCKET)
existing = []
for s in SYMBOLS:
    prefix = f"bronze/trades/symbol={s.upper()}/dt={today}/"
    if any(True for _ in bucket.list_blobs(prefix=prefix, max_results=1)):
        existing.append(f"gs://{BUCKET}/{prefix}trades_*.json")
    else:
        print(f"  (no files for {s.upper()} today, skipping)")

if not existing:
    raise SystemExit("no symbol partitions found for today")

load_job = client.load_table_from_uri(existing, TABLE, job_config=job_config)
load_job.result()   # wait for it to finish
table = client.get_table(TABLE)
print(f"loaded {table.num_rows} rows into {TABLE} from {len(existing)} symbols")
