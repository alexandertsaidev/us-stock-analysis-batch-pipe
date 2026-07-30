# US Stock Analysis — Batch Pipeline

![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=flat&logo=apacheairflow&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-FFC107?style=flat&logo=duckdb&logoColor=black)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat&logo=dbt&logoColor=white)
![MinIO](https://img.shields.io/badge/MinIO-C72E49?style=flat&logo=minio&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=flat&logo=snowflake&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=flat&logo=postgresql&logoColor=white)
![Slack Alerts](https://img.shields.io/badge/🔔%20Slack%20Alerts-4A154B?style=flat&logoColor=white)
![Google Sheets](https://img.shields.io/badge/Google%20Sheets-34A853?style=flat&logo=googlesheets&logoColor=white)

A **production-grade batch data pipeline** that scrapes US stock universe from the **SEC**, fetches fundamentals & historical prices via **yfinance**, applies multi-period technical analysis with **TA-Lib**, transforms data through **dbt + DuckDB**, and delivers actionable entry signals to **Google Sheets** — with full pipeline observability via **Slack**.

✅ **Achievement**

Built a **fully automated, scheduled stock analysis pipeline** covering the full US equity universe — from symbol discovery to entry-signal delivery — across **two independent Airflow DAGs** with zero manual intervention.

📈 **Metric**

DAG 1 runs **bi-monthly** (1st & 15th) to refresh fundamentals and screen **6 stock categories** (HighGrowth / SteadyGrowth / Distressed / Super / Magnificent 8 / ETF) via a **DuckDB SQL screener**. DAG 2 runs **every weekday at 18:00 ET** to fetch OHLCV history, compute **7 time-period technical indicators** (D / W / 2W / 3W / ME / 2ME / 3ME), and materialize a **Gold-layer entry conclusion** Parquet via dbt.

⚡ **Action**

Engineered with **Apache Airflow** (`ExternalPythonOperator`) for DAG orchestration, **yfinance** for market data ingestion, **TA-Lib** for technical indicators, **dbt-DuckDB** for SQL transformations, and **MinIO → Snowflake** for data lake to warehouse promotion — with 🔔 **Slack webhook alerts** at every pipeline stage.

---

## 📑 Table of Contents

- [Architecture](#-architecture)
- [DAG Overview](#-dag-overview)
- [Layer Responsibilities](#-layer-responsibilities)
- [SQL Reference](#-sql-reference)
- [Getting Started](#-getting-started)
  - [Prerequisites](#prerequisites)
  - [Environment Variables](#environment-variables)
  - [Installation](#installation)
- [Usage](#-usage)
- [Slack Notifications](#-slack-notifications)
- [Tech Stack](#-tech-stack)
- [License](#-license)
- [Connect](#-connect)

---

## 🏗️ Architecture

```
          ┌──────────────────────────────────────────┐
          │   Data Sources                           │
          │   SEC EDGAR (tickers)                    │
          │   yfinance (fundamentals + OHLCV)        │
          └──────────────┬───────────────────────────┘
                         │
                         ▼
          ┌──────────────────────────────────────────┐
          │   Apache Airflow                         │
          │                                          │
          │   DAG 1 (bi-monthly)  ──►  Symbols       │
          │                            Fundamentals  │
          │                            Screening     │
          │                            → GSheet      │
          │                                          │
          │   DAG 2 (weekdays)    ──►  History Fetch │
          │                            Clean         │
          │                            Calculate     │
          │                            Union         │
          │                            dbt (Gold)    │
          │                            → GSheet      │
          │                            → Snowflake   │
          └──────────────┬───────────────────────────┘
                         │
                         ▼
          ┌──────────────────────────────────────────┐
          │   MinIO  (Bronze / Silver / Gold)        │
          │   Parquet, partitioned by ticker/date    │
          └──────────────┬───────────────────────────┘
                         │
               ┌─────────┴──────────┐
               ▼                    ▼
   ┌────────────────────┐  ┌────────────────────┐
   │   Snowflake DWH    │  │   Google Sheets    │
   │   (historical data)│  │   (entry signals)  │
   └────────────────────┘  └────────────────────┘
                         │
                         ▼
          ┌──────────────────────────────────────────┐
          │   Slack Webhook                          │
          │   Pipeline alerts & stage summaries      │
          └──────────────────────────────────────────┘
```

---

## 🗓️ DAG Overview

### DAG 1 — `stock_analyzer_us_1` (Bi-monthly)

> **Schedule:** `0 19 1,15 * *`  — runs on the 1st & 15th of each month at 19:00 ET

```
co_fetcher  ──►  co_fund_fetcher  ──►  co_screener  ──►  upload_sheet
```

| Step | Module | Description |
|------|--------|-------------|
| `co_fetcher` | `company_symbols/us_co_symbol_fetcher` | Scrapes full US ticker list from SEC EDGAR; upserts to MinIO Parquet |
| `co_fund_fetcher` | `company_fundamentals/us_co_fundamentals_fetcher` | Fetches fundamentals (P/S, ROE, ROA, EPS, cashflow, analyst ratings…) via yfinance; upserts to MinIO |
| `co_screener` | `company_screening/us_co_screener` | Runs DuckDB SQL screener to classify stocks into 6 categories; upserts to MinIO |
| `upload_sheet` | `upload_pipeline/upload_sheet_2` | Writes screened results to Google Sheets; sends Slack summary |

### DAG 2 — `stock_analyzer_us_2` (Weekdays)

> **Schedule:** `0 18 * * 1-5`  — runs every weekday at 18:00 ET

```
fetcher  ──►  cleaner  ──►  calculator  ──►  unioner
                                        ──►  upload_snowflake

unioner  ──►  entry_conclusion (dbt)  ──►  upload_sheet
```

| Step | Module | Description |
|------|--------|-------------|
| `fetcher` | `company_history_prices/history_price_fetcher` | Fetches OHLCV history for screened tickers via yfinance; saves per-ticker Parquet to MinIO (Bronze) |
| `cleaner` | `company_history_prices/history_price_cleaner` | Validates and cleans raw OHLCV data; removes duplicates and anomalies |
| `calculator` | `company_history_prices/history_price_calculator` | Computes multi-period technical indicators (TA-Lib) across 7 time frames (D / W / 2W / 3W / ME / 2ME / 3ME) |
| `unioner` | `company_history_prices/history_price_union` | Merges all per-ticker Silver Parquets into a unified Gold Parquet in MinIO |
| `entry_conclusion` | `dbt_project/models/marts/entry_conclusion.sql` | dbt model: lateral-joins all 7 time periods per ticker; materializes final entry-signal Parquet to MinIO Gold layer |
| `upload_sheet` | `upload_pipeline/upload_sheet` | Writes entry conclusions to Google Sheets; sends Slack summary |
| `upload_snowflake` | `upload_pipeline/minio_to_snowflake` | Promotes today's Parquet files from MinIO to Snowflake data warehouse |

---

## 📦 Layer Responsibilities

### 1. Data Sources

**SEC EDGAR — Symbol Universe**
- Scrapes the full US company ticker list from `https://www.sec.gov/files/company_tickers.json`
- Randomized `User-Agent` headers with retry logic to avoid bot detection
- Upserts CIK, ticker, and company name to MinIO; marks delisted tickers as `is_active = False`

**yfinance — Fundamentals & OHLCV**
- Fetches per-ticker fundamentals: market cap, P/S ratio, ROE, ROA, EPS (TTM), operating cash flow, current ratio, debt/equity, institutional ownership, analyst ratings
- Fetches OHLCV historical price data for all screened tickers
- Handles `YFRateLimitError`, `YFTickerMissingError`, and `YFInvalidPeriodError` gracefully

### 2. Apache Airflow — Orchestration

- Two independent DAGs manage the bi-monthly fundamental refresh and the daily price analysis cycle
- All tasks run via `ExternalPythonOperator` using an isolated virtual environment (`/opt/venvs/us-stock-analysis-batch-pipe/bin/python`), keeping Airflow's own environment clean
- Each task raises `RuntimeError` on non-zero subprocess exit, triggering Airflow's native retry and alerting

### 3. DuckDB — In-Process Analytics Engine

- Powers all upsert logic: new data is staged as a temp Parquet, then merged with existing data using `UNION ALL` + `NOT IN` deduplication — no database server required
- Reads and writes Parquet files directly from/to MinIO via S3-compatible endpoint (`read_parquet('s3://...')`)
- Used both in Python pipeline code and as the dbt adapter (`dbt-duckdb`)

### 4. TA-Lib + Multi-Period Indicator Engine

- Computes technical indicators across **7 resampled time periods**: Daily, Weekly, 2-Week, 3-Week, Monthly, 2-Month, 3-Month
- Each period calculates `Side_1`, `Side_2`, `Side_3` signals (trend / momentum / confirmation layers)
- Configurable via `period_indicator_config.py`

### 5. dbt — SQL Transformation (Gold Layer)

- `entry_conclusion.sql`: lateral-joins all 7 period staging models per ticker on the latest available date; materializes result as an external Parquet file to the MinIO Gold layer
- Staging models (`stg_prices_D`, `stg_prices_W`, etc.) read from Silver Parquets via DuckDB
- dbt lineage tracked in `manifest.json`; docs available via `dbt docs serve`

### 6. MinIO — Data Lake (Bronze / Silver / Gold)

| Layer | Path | Content |
|-------|------|---------|
| **Bronze** | `stock/company_list/`, `stock/fundamentals/`, `stock/history/prices/raw/` | Raw fetched data, per-ticker Parquet |
| **Silver** | `stock/screening/`, `stock/history/prices/silver/` | Cleaned & calculated per-ticker Parquet |
| **Gold** | `stock/history/prices/gold/final_all/`, `stock/history/prices/gold/conclusion/` | Unified & transformed, analysis-ready Parquet |

- **S3-compatible** storage — portable to **AWS S3** with minimal config changes
- All writes use upsert semantics (temp → merge → final) to guarantee idempotency

### 7. Snowflake — Data Warehouse

- Receives today's processed Parquet files promoted from MinIO via `minio_to_snowflake.py`
- Filters only today's modified files (Taiwan time) and excludes `final_all` and `conclusion` directories to avoid duplication
- Enables downstream BI and long-term historical querying

### 8. Google Sheets — Signal Delivery

- `upload_sheet.py`: writes the final `entry_conclusion` table (sorted by date desc, ticker asc) to the `US` worksheet
- `upload_sheet_2.py`: writes the fundamental screener results
- Date columns are normalized to `YYYY-MM-DD` format; full sheet is cleared and rewritten each run
- Authenticated via Google Service Account (`credentials.json`)

### 9. Slack — Alerts & Notifications

- Receives pipeline status updates at each key stage via `SLACK_BATCH_PIPE_WEBHOOK_URL`
- Notifies on screener results summary, successful Google Sheet uploads (with sheet URL), and pipeline errors
- Keeps operators informed without needing to monitor Airflow logs manually

---

## 🗃️ SQL Reference

This project uses SQL in two distinct contexts: **DuckDB screener queries** (DAG 1) and **dbt transformation models** (DAG 2).

---

### DuckDB Screener — `us_fundamentals_screen.sql`

**File:** `src/data_pipeline/company_screening/us_fundamentals_screen.sql`

Executed by `us_co_screener.py` via an in-process DuckDB connection against the fundamentals Parquet (`stock/fundamentals/`) stored in MinIO. Classifies each ticker into one of six categories using a `CASE WHEN` expression, then filters out unmatched rows.

**Categories & key conditions:**

| Category | Key Criteria |
|---|---|
| `HighGrowth` | 市值 ≥ 1B、P/S 2–10、ROE 5–25%、流動比率 ≥ 2.5、EPS & 現金流 > 0、機構持股 40–95%、分析師評級 ≤ 3 |
| `SteadyGrowth` | 市值 ≥ 400M、P/S 2–7、ROE 15–25%、流動比率 ≥ 2、目前價格 ≥ 15、年銷售額 ≥ 50M、分析師評級 ≤ 2 |
| `Distressed` | P/S ≤ 1.5、ROE 15–25%、流動比率 ≥ 1.1、負債權益比 ≤ 50、日均成交 ≥ 50K、分析師評級 ≥ 3 |
| `Super` | 52週漲幅 ≥ 300%、52週新高率 ≥ 90%、目前價格 ≥ 15、日均成交 ≥ 50K、分析師評級 ≤ 2 |
| `Magnificent_8` | 固定名單：AAPL / MSFT / GOOGL / AMZN / NVDA / META / TSLA / NFLX |
| `ETF_1` | 固定名單：QQQ / SPY / DIA |

**Upsert pattern (DuckDB):**

The screener writes results via a temp → merge pattern to guarantee idempotency:

```sql
-- New rows not already in final (keyed on created_at + ticker)
SELECT * FROM temp_data
WHERE (created_at, ticker) NOT IN (SELECT created_at, ticker FROM final_data)
UNION ALL
-- Keep existing rows that were not updated
SELECT * FROM final_data
WHERE (created_at, ticker) NOT IN (SELECT created_at, ticker FROM temp_data)
```

---

### dbt Models — Staging & Mart

All dbt models use the **`dbt-duckdb` adapter** and read/write Parquet files directly from/to MinIO via DuckDB's S3-compatible `read_parquet()`.

#### Staging Models (`src/dbt_project/models/staging/`)

Each staging model is a lightweight `VIEW` that filters one time-period slice from the unified Silver Parquet:

**Source declaration (`sources.yml`):**
```yaml
sources:
  - name: raw
    tables:
      - name: us_all_prices
        meta:
          external_location: "s3://us-stock/stock/history/prices/gold/final_all/us_all_prices.parquet"
```

**Pattern (identical for all 7 periods):**
```sql
-- e.g. stg_prices_2W.sql
{{ config(materialized='view') }}

SELECT "ticker", "Date", "Side_1", "Side_2", "Side_3"
FROM {{ source('raw', 'us_all_prices') }}
WHERE "period" = '2W'   -- one of: D / W / 2W / 3W / ME / 2ME / 3ME
```

| Model | Period filter | Description |
|---|---|---|
| `stg_prices_D` | `'D'` | Daily indicators |
| `stg_prices_W` | `'W'` | Weekly indicators |
| `stg_prices_2W` | `'2W'` | Bi-weekly indicators |
| `stg_prices_3W` | `'3W'` | Tri-weekly indicators |
| `stg_prices_ME` | `'ME'` | Monthly (month-end) indicators |
| `stg_prices_2ME` | `'2ME'` | Bi-monthly indicators |
| `stg_prices_3ME` | `'3ME'` | Tri-monthly indicators |

#### Mart Model — `entry_conclusion.sql`

**File:** `src/dbt_project/models/marts/entry_conclusion.sql`

Materializes as an **external Parquet** file directly to MinIO Gold layer:

```sql
{{ config(
    materialized='external',
    location='s3://us-stock/stock/history/prices/gold/conclusion/entry_conclusion.parquet',
    format='parquet'
) }}
```

Uses **`LEFT JOIN LATERAL`** (DuckDB-native correlated subquery) to attach the most recent indicator row from each staging model relative to each daily candle. Each lateral join applies a time-tolerance window to gracefully handle period boundary misalignment:

| Joined period | Tolerance window |
|---|---|
| Weekly (`W`) | Daily date + 8 days |
| Bi-weekly (`2W`) | Daily date + 15 days |
| Tri-weekly (`3W`) | Daily date + 22 days |
| Monthly (`ME`) | Daily date + 32 days |
| Bi-monthly (`2ME`) | Daily date + 63 days |
| Tri-monthly (`3ME`) | Daily date + 93 days |

The final `SELECT` excludes the latest date row per ticker (to avoid incomplete in-progress candles):

```sql
SELECT * EXCLUDE ("max_date")
FROM base
WHERE "Date_D" < "max_date"
```

Output columns follow the pattern `Side_1_D`, `Side_1_W`, … `Side_3_3M` — 21 signal columns in total across 7 periods × 3 sides.

---

### DuckDB Connection Setup

Both pipeline Python code and dbt share the same S3-compatible DuckDB connection configuration (`src/config/minio_duckdb_conn.py`):

```python
conn = duckdb.connect()
conn.execute("""
    SET s3_endpoint          = '<MINIO_ENDPOINT>';
    SET s3_access_key_id     = '<MINIO_ACCESS_KEY>';
    SET s3_secret_access_key = '<MINIO_SECRET_KEY>';
    SET s3_use_ssl   = false;
    SET s3_url_style = 'path';
""")
```

This same endpoint configuration is picked up by dbt via the `dbt_project.yml` profile, ensuring that `read_parquet('s3://...')` resolves to your MinIO instance in both local runs and Airflow-triggered dbt executions.

---

## 🚀 Getting Started

### Prerequisites

- **Apache Airflow** installed and running (with `ExternalPythonOperator` support)
- **Python 3.13** virtual environment at `/opt/venvs/us-stock-analysis-batch-pipe/`
- **MinIO** instance accessible (local or remote)
- **Snowflake** account (free trial available)
- **Google Service Account** with Sheets + Drive API enabled
- **Slack incoming webhook URL** (see [Slack Notifications](#-slack-notifications))
- **`.env` file** configured (see below)

### Environment Variables

Create a `.env` file in the project root:

```env
# ── MinIO 連線設定 ──────────────────────────────────────────
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin

# ── MinIO 業務設定 ──────────────────────────────────────────
MINIO_SECURE=false

# ── bucket ──────────────────────────────────────────────────
MINIO_BUCKET=us-stock

# ── Snowflake 連線設定 ──────────────────────────────────────
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema

# ── Slack 通知設定 ──────────────────────────────────────────
SLACK_BATCH_PIPE_WEBHOOK_URL=https://hooks.slack.com/services/XXXXXXXXX/XXXXXXXXX/xxxxxxxxxxxxxxxxxxxxxxxx
SLACK_RT_PIPE_WEBHOOK_URL=https://hooks.slack.com/services/XXXXXXXXX/YYYYYYYYY/yyyyyyyyyyyyyyyyyyyyyyyyyy

# ── Google Sheet 設定 ───────────────────────────────────────
GSHEET_ENTRY_URL=https://docs.google.com/spreadsheets/d/XXXXXXXXX
GSHEET_SCREEN_URL=https://docs.google.com/spreadsheets/d/YYYYYYYYY

# ── Finnhub API 設定 ───────────────────────────────────────
FINNHUB_API_KEY=your_finnhub_api_key_here
```

### Installation

```bash
# 1. Clone the repo
git clone https://github.com/alexandertsaidev/us-stock-analysis-batch-pipe.git
cd us-stock-analysis-batch-pipe

# 2. Copy and fill in environment variables
cp .env.example .env

# 3. Create the isolated Python virtual environment
python3.13 -m venv /opt/venvs/us-stock-analysis-batch-pipe
source /opt/venvs/us-stock-analysis-batch-pipe/bin/activate

# 4. Install dependencies
pip install poetry
poetry install

# 5. Place the project under Airflow scripts directory
cp -r . /opt/airflow/scripts/us-stock-analysis-batch-pipe

# 6. Copy DAG files to Airflow DAGs folder
cp dags/*.py $AIRFLOW_HOME/dags/

# 7. Place Google Service Account credentials
# Put your credentials.json at:
# src/upload_pipeline/credentials.json

# 8. Trigger DAGs manually (first run) or let the schedule take over
airflow dags trigger stock_analyzer_us_1
airflow dags trigger stock_analyzer_us_2
```

---

## 📖 Usage

Once deployed, the Airflow scheduler manages all execution automatically.

**Manually trigger a DAG:**
```bash
airflow dags trigger stock_analyzer_us_1
airflow dags trigger stock_analyzer_us_2
```

**Check task logs:**
```bash
airflow tasks logs stock_analyzer_us_2 fetcher <execution_date>
```

**Check MinIO for stored data:**
Open your MinIO console and inspect the `us-stock` bucket. Data is organized by layer and object type:

```
us-stock/
├── stock/company_list/us_tickers_list.parquet          ← Bronze: SEC tickers
├── stock/fundamentals/temp_us_co_fundamentals.parquet  ← Bronze: yfinance fundamentals
├── stock/screening/us_all_co_screen.parquet            ← Silver: screened results
├── stock/history/prices/raw/<ticker>.parquet           ← Bronze: raw OHLCV
├── stock/history/prices/gold/final_all/                ← Gold: unified prices
└── stock/history/prices/gold/conclusion/
    └── entry_conclusion.parquet                        ← Gold: final entry signals
```

**Run dbt models manually:**
```bash
cd src/dbt_project
dbt run --select +entry_conclusion
dbt docs generate && dbt docs serve
```

---

## 🔔 Slack Notifications

This pipeline sends stage-by-stage alerts to a Slack channel via incoming webhook.

**Setup:**
1. Go to [Slack API: Incoming Webhooks](https://api.slack.com/messaging/webhooks)
2. Create a new app → **Enable Incoming Webhooks** → Add to a channel
3. Copy the webhook URL into your `.env` as `SLACK_BATCH_PIPE_WEBHOOK_URL`

**Notification events:**
- 🎯 **Screener results** — category breakdown with ticker count and date (DAG 1)
- 🔗 **Google Sheet upload** — confirmation with direct sheet URL (DAG 1 & 2)
- ✅ **Successful stage completion** — per-task summary with record counts
- ❌ **Pipeline errors** — task name and error summary for rapid diagnosis

---

## 🧰 Tech Stack

| Layer | Technology |
|---|---|
| **Data Sources** | SEC EDGAR (HTTP scrape), yfinance |
| **Orchestration** | Apache Airflow (ExternalPythonOperator) |
| **Ingestion & Transform** | Python, pandas, PyArrow |
| **Technical Analysis** | TA-Lib, scipy, numpy |
| **In-Process Analytics** | DuckDB |
| **SQL Transformation** | dbt (dbt-duckdb adapter) |
| **Data Lake Storage** | MinIO (S3-compatible, Parquet) |
| **Data Warehouse** | Snowflake |
| **Signal Delivery** | Google Sheets (gspread) |
| **Alerts** | Slack Incoming Webhook |
| **Dependency Management** | Poetry (Python 3.13) |

---

## 📄 License

This project is licensed under the [MIT License](LICENSE).

---

## 📬 Connect

[![Hotmail](https://img.shields.io/badge/Hotmail-0078D4?style=flat&logo=microsoft-outlook&logoColor=white)](mailto:caiyuexun.hcd520201@hotmail.com)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=flat&logo=linkedin&logoColor=white)](https://linkedin.com/in/alexander-tsai-tw-eu)
