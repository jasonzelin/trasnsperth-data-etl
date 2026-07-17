# Transperth Public Transport Data Pipeline

An end-to-end data engineering portfolio project that ingests, transforms, and visualises Perth's public transport network data using the modern data stack.

**[📊 View Live Dashboard](https://datastudio.google.com/reporting/a40d9d25-a4e0-4ac4-8b33-2922280c150b)** &nbsp;|&nbsp;
**[📖 dbt Docs](https://jasonzelin.github.io/transperth-data-etl/)**
---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        ORCHESTRATION                            │
│                    Apache Airflow (local)                       │
│          runs daily at 6am AWST on a scheduled DAG             │
└────────────────────────┬────────────────────────────────────────┘
                         │
          ┌──────────────┼──────────────┐
          ▼              ▼              ▼
   ┌─────────────┐ ┌──────────┐ ┌──────────────┐
   │  INGESTION  │ │  LOAD    │ │  TRANSFORM   │
   │             │ │          │ │              │
   │  Python     │ │ BigQuery │ │     dbt      │
   │  requests   │ │ (bronze) │ │              │
   │  pandas     │ │          │ │ bronze       │
   │             │ │          │ │   → silver   │
   │ GTFS Static │ │          │ │     → gold   │
   │ Feed (ZIP)  │ │          │ │              │
   └──────┬──────┘ └────┬─────┘ └──────┬───────┘
          │              │              │
          ▼              ▼              ▼
   ┌─────────────────────────────────────────────┐
   │              Google BigQuery                │
   │                                             │
   │  bronze  →  silver  →  gold                 │
   │  (raw)      (clean)    (aggregated)         │
   └─────────────────────────┬───────────────────┘
                             │
                             ▼
                  ┌─────────────────────┐
                  │   Looker Studio     │
                  │   Dashboard         │
                  │                     │
                  │  Network Overview   │
                  │  Route Analysis     │
                  │  Stop Frequency Map │
                  └─────────────────────┘
```

### Data Flow

**1. Ingest** — A Python script downloads the Transperth GTFS static feed (a ZIP file published by the Public Transport Authority of Western Australia) and extracts six tables: `routes`, `stops`, `trips`, `stop_times`, `calendar`, and `calendar_dates`. Each table is cleaned and saved as a CSV to a local `data/raw/` folder. A `manifest.json` records the run timestamp and row counts.

**2. Load** — An Airflow task uploads the CSVs from `data/raw/` into BigQuery's `bronze` dataset, overwriting the previous run's data on each execution.

**3. Transform** — dbt runs three layers of transformation inside BigQuery:
- **Bronze** — raw source tables, no transformation, direct reflection of the GTFS feed
- **Silver** — cleaned and typed models, one per source table, with standardised column names, cast data types, and derived labels
- **Gold** — aggregated mart tables that answer specific business questions, used directly by the dashboard

**4. Visualise** — Looker Studio connects directly to the gold dataset and serves an interactive three-page dashboard.

---

## Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Ingestion | Python (`requests`, `pandas`, `google-cloud-bigquery`) | Download and parse Transperth GTFS feed |
| Orchestration | Apache Airflow 3.3 | Schedule and sequence pipeline tasks regularly |
| Data Warehouse | Google BigQuery | Store and compute all data layers |
| Transformation | dbt Core | Model bronze → silver → gold layers |
| Testing | dbt tests | Schema tests and unit tests for ingestion functions |
| Visualisation | Looker Studio | Interactive dashboard on gold layer |
| CI/CD | GitHub Actions | Auto-deploy dbt docs to GitHub Pages on push to main |
| Version Control | Git + GitHub | Source control and documentation hosting |

---

## Project Structure

```
transperth-data-etl/
├── config.json                  # pipeline configuration (gitignored)
├── config.example.json          # template — copy to config.json
├── main.py                      # pipeline entry point
├── utils.py                     # ingestion functions
├── tests/
│   └── test_utils.py            # pytest unit tests
├── data/
│   └── raw/                     # ingested CSVs (gitignored)
├── dbt_transperth/
│   ├── dbt_project.yml
│   ├── profiles.yml             # BigQuery connection (gitignored)
│   ├── macros/
│   │   └── generate_schema_name.sql   # overrides dbt schema naming
│   └── models/
│       ├── bronze/              # raw source models
│       │   └── sources.yml
│       ├── silver/              # cleaned and typed models
│       │   └── schema.yml
│       └── gold/                # aggregated mart models
│           └── schema.yml
├── airflow/
│   └── dags/
│       └── transperth_pipeline_dag.py
└── .github/
    └── workflows/
        └── dbt_docs.yml         # auto-deploy dbt docs to GitHub Pages
```

---

## Data Source

**Transperth GTFS Static Feed**
Published by the Public Transport Authority of Western Australia and freely available at [transperth.wa.gov.au](https://www.transperth.wa.gov.au/About/Spatial-Data-Access). Updated regularly and used by Google Maps to power transit directions in Perth.

| Table | Description | Approx. rows count |
|---|---|---|
| `routes` | All bus, rail, and ferry routes | ~435 |
| `stops` | All physical stops and stations with coordinates | ~14,000 |
| `trips` | Individual scheduled trips per route | ~85,000 |
| `stop_times` | Arrival and departure times at each stop | ~2,000,000+ |
| `calendar` | Weekly service patterns | ~20 |
| `calendar_dates` | Schedule exceptions (public holidays) | ~100 |

---

## How to Run Locally

### Prerequisites

- Python 3.11+
- A Google Cloud Platform account with a BigQuery-enabled project
- A GCP service account JSON key with `BigQuery Admin` role
- Apache Airflow 3.3+

### 1. Clone the repository

```bash
git clone https://github.com/jasonzelin/transperth-data-etl.git
cd transperth-data-etl
```

### 2. Set up the Python environment

```bash
python -m venv airflow_env
source airflow_env/bin/activate # For Mac/Linux
pip install -r requirements.txt
```

### 3. Configure the pipeline

Edit `config.json` with your preferred output directory and timeout settings. The GTFS URL is pre-filled and does not need to change.

### 4. Set environment variables

```bash
export GCP_PROJECT_ID=your-gcp-project-id
export GOOGLE_APPLICATION_CREDENTIALS=~/.gcp/your-service-account.json
```

### 5. Run the ingestion script manually

```bash
python main.py
```

CSVs will be saved to `data/raw/` and a `manifest.json` will be written alongside them.

### 6. Set up dbt

```bash
pip install dbt-bigquery
cd dbt_transperth
```

Copy the profiles example and fill in your GCP project details:

```bash
cp profiles_example.yml profiles.yml
```

Run dbt:

```bash
dbt debug          # confirm BigQuery connection
dbt run            # build all models
dbt test           # run schema tests
dbt docs generate  # generate documentation
dbt docs serve     # open docs in browser at localhost:8080
```

### 8. Set up and run Airflow

```bash
cd ..
export AIRFLOW_HOME=$(pwd)/airflow
airflow db migrate
airflow standalone
```

Open `http://localhost:8080` in your browser, log in with the printed credentials, and enable the `transperth_pipeline` DAG. Trigger a manual run to test the full pipeline end to end.

---

## Dashboard

The Looker Studio dashboard connects directly to the BigQuery gold layer and shows high-level descriptive numbers of the Transperth public transport network. In summary, the dashboard shows 3 parts of visualizations: (1) Total numbers of routes, trips, stops and arrivals; (2) Route distribution per transport mode & Top 10 Routes; (3) Geomap of stops served by Transperth.

👉 **[Open the live dashboard](https://datastudio.google.com/reporting/a40d9d25-a4e0-4ac4-8b33-2922280c150b)**

---

## What I'd Improve at Scale

These are the next steps I'd take if this pipeline were running in a production environment rather than a portfolio project:

**Incremental loading** — The current pipeline does a full truncate-and-reload of every table on each run. At production scale, dbt incremental models would only process new or changed records, significantly reducing BigQuery compute costs and run time.

**Separate virtual environments per task** — Airflow's `ExternalPythonOperator` would allow each task to run in its own isolated Python environment, preventing dependency conflicts between the ingestion libraries and dbt as the project grows.

**Secrets management** — Environment variables work for local development but production would use Google Secret Manager, Amazon S3 Secrets Manager or HashiCorp Vault to manage the service account credentials, with Airflow's connections store used instead of environment variables.

**Data monitoring** — The GTFS feed is updated periodically but not on a fixed schedule. A freshness check step in the DAG (using dbt's `source freshness` command) would alert if the source data hasn't changed within an expected window, catching silent feed failures before they propagate to the dashboard. I would setup something like alert system to trigger warning to messenger platforms such as Google Chat's or Slack's webhook.

**Cloud-hosted Airflow** — Running Airflow locally means the pipeline only runs when my machine is on and hosting the source code. Cloud Composer (GCP's managed Airflow) or Astronomer would give the pipeline true 24/7 scheduling reliability for real use cases.

---

## Author

**Jason Zelin**
Data Engineer | Perth, WA
[GitHub](https://github.com/jasonzelin) · [LinkedIn](https://linkedin.com/in/jason-zelin)