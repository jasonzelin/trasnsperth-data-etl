import json
from pathlib import Path
from datetime import datetime
import utils

# Load config from root directory
config_path = Path(__file__).parent / "config.json"

if not config_path.exists():
    raise FileNotFoundError(
        f"config.json not found at {config_path}. "
        "Create or copy json values to config.json and fill in your values."
    )

with open(config_path) as f:
    CONFIG = json.load(f)

def main():
    logger = utils.setup_logging(CONFIG["log_dir"])
    logger.info("Starting Transperth GTFS ingestion")

    # 1. Download
    zip_bytes = utils.download_gtfs_zip(
        url=CONFIG["gtfs_url"],
        timeout=CONFIG["request_timeout_seconds"],
        logger=logger,
    )

    # 2. Extract
    dataframes = utils.extract_gtfs_files(
        zip_bytes=zip_bytes,
        target_files=CONFIG["target_files"],
        logger=logger,
    )

    # 3. Clean
    dataframes = utils.clean_all(dataframes, logger)

    # 4. Save
    manifest = utils.save_to_raw(
        dataframes=dataframes,
        output_dir=CONFIG["output_dir"],
        logger=logger,
    )

    # 5. Summary
    utils.print_summary(manifest, logger)
    logger.info("Ingestion complete ✓")

    # 6. Ingest to BigQuery
    utils.ingest_to_bigquery(dataframes, logger)

if __name__ == "__main__":
    main()