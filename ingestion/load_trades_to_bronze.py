import datetime as dt
from google.cloud import bigquery

PROJECT = "dm2-crypto-microstructure"
BUCKET  = "dm2-crypto-microstructure-raw"
TABLE   = f"{PROJECT}.bronze.trades_raw"

# today's partition, matches where the ingestion script wrote
today = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
uri = f"gs://{BUCKET}/bronze/trades/symbol=*/dt={today}/trades_*.json"

client = bigquery.Client(project=PROJECT)

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    autodetect=True,
    write_disposition="WRITE_TRUNCATE",   # replace, same as --replace
)

print(f"loading {uri} into {TABLE}")
load_job = client.load_table_from_uri(uri, TABLE, job_config=job_config)
load_job.result()   # wait for it to finish

table = client.get_table(TABLE)
print(f"loaded {table.num_rows} rows into {TABLE}")
