# INS Assignment – End‑to‑End Data Engineering Pipeline (GCS → Databricks DWH)

[![Repo](https://img.shields.io/badge/GitHub-INS__Assignment-black?logo=github)](https://github.com/Prateekjha09/INS_Assignment)
[![Language](https://img.shields.io/badge/Python-3.11+-blue?logo=python)](https://www.python.org/)
[![Platform](https://img.shields.io/badge/Platform-Databricks-orange)](https://www.databricks.com/)
[![Cloud](https://img.shields.io/badge/Storage-Google%20Cloud%20Storage-4285F4?logo=googlecloud&logoColor=white)](https://cloud.google.com/storage)

> End‑to‑end data pipeline that ingests raw CSV data from Google Cloud Storage (GCS), lands it in Databricks Delta Lake (silver and gold layers with SCD‑2), and powers advanced analytics and Power BI dashboards for a news/content app.

---

## 1. Project Overview

This project demonstrates a **production‑style data engineering pipeline** for a content/news application with three core entities:

- **users** – app users / devices.
- **events** – user interactions with content (Opened, Shown, Front, Back, Shared).
- **content** – news/content items.

## Architecture:
![Data Pipeline](https://github.com/Prateekjha09/INS_Assignment/blob/F1/PowerBI_Report/SystemDesignForDataPipeline.jpg)

### High‑level flow

1. **Ingestion (Python)**  
   - Read raw CSVs from **GCS buckets** (users, events, content).  
   - Persist raw data as **local Parquet** without type conversion (bronze/raw).

2. **Databricks lakehouse (Delta Lake)**  
   - Load Parquet into **silver Delta tables** (clean base tables).  
   - Copy into **gold staging tables** with SCD‑2 scaffolding columns:
     - `effective_from`, `effective_to`, `is_active`.  
   - Use SQL MERGE logic to maintain **gold final SCD‑2 dimensions** and event fact.

3. **Analytics & BI**  
   - Use **Databricks SQL + notebooks** for:
     - retention (D1, D2, W1, M1, M3),
     - churn (weekly/monthly),
     - engagement funnels and cohort analysis.  
   - Expose **gold tables** to **Power BI**:
     - star‑like model with `users` and `content` as dims, `events` as fact.
     - dashboards for user acquisition, platform mix, engagement.

Repository link: <https://github.com/Prateekjha09/INS_Assignment>

---

## 2. Repository Structure
![](https://github.com/Prateekjha09/INS_Assignment/blob/F1/PowerBI_Report/FileTree.jpg)



### Notebook flow (Databricks)

The `notebooks/` folder contains Databricks notebooks in the **execution order of the pipeline**:

1. **1_Data_Understanding.ipynb**  
   Data profiling, schema understanding, volume checks for users, events, content.

2. **2_Table_Creation.ipynb**  
   Create **silver** Delta tables, **gold staging**, and **gold final** SCD‑2 tables in Databricks.

3. **3_SQL_Queries.ipynb**  
   Core SQL logic: joins, event funnels, D1/D2/W1/M1/M3 retention, churn, cohort metrics.

4. **4_Retention_And_Churn_Analysis.ipynb**  
   Higher‑level analytics for product/marketing (retention curves, churn breakdowns, platform and language insights).

Data flows:  
**Silver tables → Gold staging tables → Gold final SCD‑2 tables → Advanced analytics.**

---

## 3. Architecture & Data Flow

Pipeline stages:

1. **GCS → Bronze/Raw (local Parquet)**  
   - Python script (`utils/dataIngestFromGCS.py`) pulls CSV from GCS prefixes:
     - `user/`, `event/`, `content/`.  
   - Writes:
     - `user.parquet`, `event.parquet`, `content.parquet`.  
   - All columns are read as `string` to preserve raw fidelity.

2. **Bronze/Raw → Silver (Databricks)**  
   - Silver Delta tables created from Parquet:
     - Normalized schemas, correct types,
     - basic cleaning (e.g., null handling, timestamp conversion).

3. **Silver → Gold Staging**  
   - Streaming/batch SQL moves data from silver to gold staging.  
   - Adds SCD‑2 columns:
     - `effective_from` (current date),
     - `effective_to` (`9999-12-31`),
     - `is_active` (`TRUE`).

4. **Gold Staging → Gold Final (SCD‑2)**  
   - SQL MERGE logic per table:
     - `users` and `content` as SCD‑2 dimensions.  
     - `events` as fact referencing `users.deviceid` and `content._id`.  
   - Historical versions captured by closing old rows and inserting new active versions.

5. **Gold Final → Dashboards & ML**  
   - Gold tables serve:
     - Power BI dashboards (user acquisition, engagement, funnels).  
     - Retention/churn analysis in Databricks.  
     - Future ML/DS use cases (e.g., propensity, personalization).

---

## 4. Prerequisites

- **Git** installed.
- **Python** 3.11+.
- **Databricks workspace** (community or paid) with:
  - Access to a cluster,
  - Unity Catalog / DBFS / Volumes for Delta tables.
- **Google Cloud Platform**:
  - GCS bucket with CSV files:
    - `user/`, `event/`, `content/`.
  - Service account JSON with read access to the bucket.
- **MySQL** (optional, only if extending to RDBMS sink).

---

## 5. Getting Started

### 5.1 Clone the repository

1) Configure your Git identity (run once)
git config --global user.name "YourName"
git config --global user.email "your_email@example.com"

2) Clone repo
cd path/to/your/workspace
git clone https://github.com/Prateekjha09/INS_Assignment.git
cd INS_Assignment


If you ever initialize from scratch and attach this remote:

git init
git remote add origin https://github.com/Prateekjha09/INS_Assignment.git
git fetch
git checkout main # or the default branch used in the repo


### 5.2 Create and activate virtual environment

Windows
python -m venv my_env
my_env\Scripts\activate

Linux / macOS
python -m venv my_env
source my_env/bin/activate

Install dependencies
pip install -r requirements.txt


---

## 6. Configuration

Edit `config/config.ini` to point to your environment.

Example:

[GCS_Connection]
PROJECT_ID = your_gcp_project_id
BUCKET_NAME = your_gcs_bucket_name
USER_PREFIX = user/
EVENT_PREFIX = event/
CONTENT_PREFIX = content/


Only `GCS_Connection` is required for the core GCS → Databricks pipeline. The MySQL section is optional for later RDBMS integration.

---

## 7. Key Components

### 7.1 Configuration loader – `utils/configLoader.py`

Responsibilities:

- Resolve `config.ini` path relative to project root.
- Read the `GCS_Connection` section.
- Return a `GCSConfig` NamedTuple:

from utils.configLoader import load_config

gcs_config = load_config()
print(gcs_config.project_id)


Fields:

- `project_id`
- `bucket_name`
- `user_prefix`
- `event_prefix`
- `content_prefix`

(MySQL helpers can be enabled if that section is used.)

### 7.2 Logging – `utils/logging.py`

Responsibilities:

- Create a rotating file logger using `RotatingFileHandler`.
- Log file location: `CheckLogs/Logs_upto/YYYY-MM-DD.log`.
- Used across pipeline components for consistent logging.

Usage:


from utils.logging import log_object_creation

logger = log_object_creation()
logger.info("Pipeline started")


### 7.3 GCS ingestion – `utils/dataIngestFromGCS.py`

Core elements:

- `RawGCSLoader` class:
  - Lists CSV blobs under a given prefix.
  - Downloads and reads each file concurrently using `ThreadPoolExecutor`.
  - Loads CSV to pandas with **all columns as strings** (no type conversion).
  - Concatenates DataFrames into a single DataFrame per domain.

- `ingestion_initialization()` function:
  - Instantiates `RawGCSLoader` with `PROJECT_ID`.
  - Loads data for:
    - `USER_PREFIX` → `user.parquet`
    - `CONTENT_PREFIX` → `content.parquet`
    - `EVENT_PREFIX` → `event.parquet`
  - Returns `True` on success.

Example:


from utils.dataIngestFromGCS import ingestion_initialization

if ingestion_initialization():
print("Data ingestion completed successfully.")


---

## 8. Running the Pipeline (Python side)

The central entry point is **`main.py`**.


from utils.dataIngestFromGCS import ingestion_initialization


#### Step 1: GCS → local Parquet
def main():
    - if ingestion_initialization():
    - print("Data ingestion completed successfully.")

- if name == "main":
- main()

#### Step 2 (optional): move local Parquet to another store / trigger Databricks jobs

### Steps to run


From repo root
activate your virtual environment # see section 5.2
python main.py


Outputs (in repo root):

- `user.parquet`
- `content.parquet`
- `event.parquet`

These Parquet files are the **bronze/raw layer** inputs for Databricks.

---

## 9. Databricks: Silver and Gold Layers

Once Parquet files are available to Databricks (via a Volume, DBFS upload, or mounted storage):

1. Open the notebooks in `notebooks/` in Databricks.
2. Run them in order:

   1. **1_Data_Understanding.ipynb**  
      - Explore schema, nulls, distributions, row counts.

   2. **2_Table_Creation.ipynb**  
      - Create:
        - Silver Delta tables (users, events, content).
        - Gold staging tables (with SCD‑2 columns).
        - Gold final SCD‑2 tables (dimensions + fact).

   3. **3_SQL_Queries.ipynb**  
      - Implement:
        - D1, D2, W1, M1, M3 retention metrics.
        - Weekly and monthly churn rates by platform, language, district.
        - Funnels from `Shown → Opened → Shared`.

   4. **4_Retention_And_Churn_Analysis.ipynb**  
      - Visualize and interpret the metrics.
      - Support product decisions and marketing insights.

Relationships in the gold layer:

- `users` (dim) – SCD‑2, keyed by `deviceid`.
- `content` (dim) – SCD‑2, keyed by `_id`.
- `events` (fact) – references:
  - `events.deviceid` → `users.deviceid`
  - `events.content_id` → `content._id`

---

## 10. Power BI Model & Dashboards

### Data model

- Import `users`, `events`, `content` from Databricks gold layer (e.g., via Databricks connector or exported views).
- Relationships:
  - `users[deviceid]` (1) → `events[deviceid]` (*)
  - `content[_id]` (1) → `events[content_id]` (*)

This forms a **star‑like schema**: users and content as dimensions, events as the central fact.

### Example dashboards

- **User acquisition and distribution**
  - New vs active users by district.
  - Total users by platform (Android vs iOS).
  - Detailed user table: deviceid, district, lang, platform, campaign_id.

- **Engagement & retention**
  - D1/D7/D30/M3 retention curves.
  - Churn by platform/language/district.
  - Event funnels (Shown → Opened → Front/Back → Shared).

Gold tables (`users`, `events`, `content`) are designed specifically to support these analytics with clean SCD‑2 history.

---

## 11. How This Pipeline Can Be Extended

- Add job orchestration (e.g., Databricks Jobs, Airflow).
- Push cleaned data into a relational DWH (Snowflake, BigQuery, MySQL) using additional connectors.
- Introduce CI/CD for:
  - SQL scripts and notebooks,
  - Data quality tests (e.g., Great Expectations).
- Build ML models on top of gold fact/dim tables for:
  - churn prediction,
  - user segmentation,
  - content recommendation.


#### A very simple Dashboard based on this dataset (more can be definitely explored):
![alt text](https://github.com/Prateekjha09/INS_Assignment/blob/F1/PowerBI_Report/SampleDashboardForUsersOnly.jpg)

#### which is based on this relationship established as per my data understanding and in Power BI ofcourse:
![alt text](https://github.com/Prateekjha09/INS_Assignment/blob/F1/PowerBI_Report/PowerBi_relationship.jpg)
---

## 12. Summary

This repository showcases a **complete, production‑style data engineering project**:

- GCS ingestion with robust configuration and logging.
- Layered Databricks architecture (bronze → silver → gold staging → gold SCD‑2).
- Rich analytics for user behavior, retention, and content performance.
- Power BI dashboards connected to clean, modeled Delta tables.

It is intended as a portfolio‑quality example of how to design, implement, and document a realistic data pipeline for a modern analytics stack.


