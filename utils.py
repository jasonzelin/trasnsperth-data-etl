import io
import os
import json
import logging
import zipfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from google.cloud.bigquery.client import Client
import pandas_gbq

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------
def setup_logging(log_dir: str) -> logging.Logger:
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    log_file = Path(log_dir) / f"ingest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(),          # also print to console
        ],
    )
    return logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# STEP 1 — Download the GTFS ZIP
# ---------------------------------------------------------------------------
def download_gtfs_zip(url: str, timeout: int, logger: logging.Logger) -> bytes:
    """
    Download the GTFS ZIP from Transperth's public URL.
    Returns raw bytes so we can read it in-memory without touching disk.
    """
    logger.info(f"Downloading GTFS feed from: {url}")
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()                           # raises on 4xx/5xx
    size_mb = len(response.content) / (1024 * 1024)
    logger.info(f"Download complete — {size_mb:.1f} MB received")
    return response.content

# ---------------------------------------------------------------------------
# STEP 2 — Inspect ZIP contents and extract target files
# ---------------------------------------------------------------------------
def extract_gtfs_files(
    zip_bytes: bytes,
    target_files: list[str],
    logger: logging.Logger,
) -> dict[str, pd.DataFrame]:
    """
    Open the ZIP in-memory, list all files (logged for transparency),
    then extract only the target files into pandas DataFrames.
    Returns a dict of {filename_without_extension: DataFrame}.
    """
    dataframes = {}

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        all_files = zf.namelist()
        logger.info(f"Files found in ZIP: {all_files}")

        for filename in target_files:
            if filename not in all_files:
                logger.warning(f"Expected file not found in ZIP: {filename} — skipping")
                continue

            with zf.open(filename) as f:
                df = pd.read_csv(f, dtype=str)          # read all as str first; cast types below
                table_name = filename.replace(".txt", "")
                dataframes[table_name] = df
                logger.info(f"Loaded '{filename}' — {len(df):,} rows, {len(df.columns)} columns")

    return dataframes

# ---------------------------------------------------------------------------
# STEP 3 — Basic cleaning per table
# ---------------------------------------------------------------------------
def clean_routes(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip()
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)  # strip whitespace
    df["route_type"] = pd.to_numeric(df["route_type"], errors="coerce")
    # GTFS route_type codes: 0=Tram, 1=Metro/Rail, 2=Rail, 3=Bus, 4=Ferry
    df["route_type_label"] = df["route_type"].map({
        0: "Tram", 1: "Rail", 2: "Rail", 3: "Bus", 4: "Ferry"
    })
    return df


def clean_stops(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip()
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    df["stop_lat"] = pd.to_numeric(df["stop_lat"], errors="coerce")
    df["stop_lon"] = pd.to_numeric(df["stop_lon"], errors="coerce")
    # Flag stops missing coordinates
    df["_has_coordinates"] = df["stop_lat"].notna() & df["stop_lon"].notna()
    return df


def clean_trips(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip()
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    # direction_id: 0 = outbound, 1 = inbound
    df["direction_id"] = pd.to_numeric(df["direction_id"], errors="coerce")
    df["direction_label"] = df["direction_id"].map({0: "Outbound", 1: "Inbound"})
    return df


def clean_stop_times(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip()
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    # stop_sequence is numeric; arrival/departure kept as strings
    df["stop_sequence"] = pd.to_numeric(df["stop_sequence"], errors="coerce")
    return df


def clean_calendar(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip()
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    # Convert GTFS date strings (YYYYMMDD) to proper dates
    for col in ["start_date", "end_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format="%Y-%m-%d", errors="coerce")
    day_cols = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    for col in day_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df


def clean_calendar_dates(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip()
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d", errors="coerce")
    # exception_type: 1 = service added, 2 = service removed
    df["exception_type"] = pd.to_numeric(df["exception_type"], errors="coerce")
    df["exception_label"] = df["exception_type"].map({1: "Added", 2: "Removed"})
    return df


# Map table names to their cleaning function
CLEANERS = {
    "routes": clean_routes,
    "stops": clean_stops,
    "trips": clean_trips,
    "stop_times": clean_stop_times,
    "calendar": clean_calendar,
    "calendar_dates": clean_calendar_dates,
}


def clean_all(dataframes: dict[str, pd.DataFrame], logger: logging.Logger) -> dict[str, pd.DataFrame]:
    cleaned = {}
    for name, df in dataframes.items():
        if name in CLEANERS:
            logger.info(f"Cleaning '{name}'...")
            cleaned[name] = CLEANERS[name](df)
        else:
            logger.info(f"No cleaner defined for '{name}' — saving as-is")
            cleaned[name] = df
    return cleaned

# ---------------------------------------------------------------------------
# STEP 4 — Save to local raw layer + write a run manifest (JSON)
# ---------------------------------------------------------------------------
def save_to_raw(
    dataframes: dict[str, pd.DataFrame],
    output_dir: str,
    logger: logging.Logger,
) -> dict:
    
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    manifest = {
        "run_timestamp": datetime.utcnow().isoformat() + "Z",
        "source": "Transperth GTFS Static Feed",
        "tables": {},
    }

    for name, df in dataframes.items():
        output_path = Path(output_dir) / f"{name}.csv"
        df.to_csv(output_path, index=False)
        logger.info(f"Saved '{name}' → {output_path}  ({len(df):,} rows)")
        manifest["tables"][name] = {
            "rows": len(df),
            "columns": list(df.columns),
            "output_path": str(output_path),
        }

    # Write manifest JSON alongside the CSVs
    manifest_path = Path(output_dir) / "manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2, default=str)
    logger.info(f"Manifest written → {manifest_path}")

    return manifest

# ---------------------------------------------------------------------------
# STEP 5 — Print a quick summary to console
# ---------------------------------------------------------------------------
def print_summary(manifest: dict, logger: logging.Logger) -> None:
    logger.info("=" * 55)
    logger.info("INGESTION SUMMARY")
    logger.info(f"  Run time : {manifest['run_timestamp']}")
    logger.info(f"  Source   : {manifest['source']}")
    logger.info("-" * 55)
    for table, meta in manifest["tables"].items():
        logger.info(f"  {table:<20} {meta['rows']:>8,} rows   {len(meta['columns'])} cols")
    logger.info("=" * 55)

# ---------------------------------------------------------------------------
# STEP 6 — Ingest the raw data to BigQuery
# ---------------------------------------------------------------------------
def ingest_to_bigquery(
    dataframes: dict[str, pd.DataFrame],
    logger: logging.Logger,
) -> None:
    
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'service_account.json'
    client = Client()

    logger.info("Starting ingestion to BigQuery...")
    logger.info("-" * 55)
    for name, df in dataframes.items():
        table_id = f"{client.project}.google_transit.{name}"
        
        logger.info(f"Starting uploading table {table_id} to BigQuery...")
        pandas_gbq.to_gbq(
            dataframe=df,
            destination_table=table_id,
            if_exists='replace',
            bigquery_client=client
        )
        logger.info(f"Data successfully ingested into BigQuery table: {table_id}!")
    logger.info("-" * 55)
    logger.info("All tables successfully ingested into BigQuery. Ingestion complete ✓")