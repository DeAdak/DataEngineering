# Cloud-Based Healthcare Data Platform (GCP Medallion Pipeline)

An end-to-end, production-ready healthcare data engineering platform built on Google Cloud Platform (GCP). This project implements a three-tier **Medallion Lakehouse Architecture (Bronze → Silver → Gold)** to process structured patient encounters, institutional claims, and hospital transactions from relational operational databases into analytical data marts.

---
<img width="1532" height="688" alt="Flowchart" src="https://github.com/user-attachments/assets/1acc19ad-71fb-4945-91da-897c3a27ee05" />

## 🏗️ System Architecture & Data Flow

The data engineering pipeline executes sequentially across the following layers:

[Cloud SQL (MySQL/Postgres)] ➔ [Google Cloud Storage (GCS)] ➔ [BigQuery Bronze (Raw)] ➔ [BigQuery Silver (SCD Type 2)] ➔ [BigQuery Gold (Marts)] ➔ [Reporting & Dashboards]


1. **Ingestion Zone**: Scheduled extraction routines pull historical transactional data from operational **Cloud SQL** databases and stage them as flat files inside secure **Google Cloud Storage (GCS)** landing buckets.
2. **Bronze Layer (Raw Storage)**: Raw objects are appended directly into BigQuery tables with historical preservation schemas.
3. **Silver Layer (Cleaned & Historical Tracking)**: Data undergoes quality screening (null field identification and quarantine flagging). Changed rows are captured using a single-pass **SCD Type 2 (Slowly Changing Dimension)** split-row execution block utilizing `FARM_FINGERPRINT` delta hashing.
4. **Gold Layer (Analytical Marts)**: Aggregated, business-level data structures optimized for business intelligence reporting.
5. **Orchestration**: End-to-end lifecycle automation, parallel processing, and resource cleanups managed via a parent-child master **Apache Airflow (Cloud Composer)** DAG framework.

---

## 📁 Repository Structure & Implementation Roadmap

The project is structured sequentially according to the data lifecycle:

*   **`1. Data Source Cloud SQL/`**: Production-like environment setup script configuring operational MySQL/PostgreSQL instances containing raw relational records.
*   **`2. Ingestion from Cloud SQL to GCS/`**: Cloud-native batch scripts handling external storage handshakes and migrating relational data to object storage landing zones.
*   **`3. Load Claims Data to BQ/`**: Schema definition scripts staging financial clinical claims arrays directly into the cloud warehouse.
*   **`4. Create Bronze DataSet/`**: BigQuery initialization statements creating entry point operational schemas with append-only configurations.
*   **`5. Create Silver DataSet/`**: Advanced SQL scripts managing clinical quality-gate boolean checks and executing high-performance SCD Type 2 tracking over daily partition blocks.
*   **`6. Create Gold DataSet/`**: High-performance views and analytical tables combining cleaned dimensions into optimized metrics for clinical and operational reporting.
*   **`7. Airflow to automate jobs/`**: Enterprise-grade orchestration pipelines deploying temporary Dataproc (PySpark) compute clusters to parse parallel processes safely.

---

## 🛠️ Tech Stack & Key Paradigms

*   **Cloud Infrastructure**: Google Cloud Platform (GCP), Cloud Storage (GCS)
*   **Orchestration**: Apache Airflow / Google Cloud Composer
*   **Compute Processing**: Apache Spark / PySpark / Google Cloud Dataproc
*   **Data Warehousing**: Google BigQuery (SQL, Partitioning & Clustering Optimization)
*   **Data Modeling Patterns**: Medallion Architecture, Slowly Changing Dimensions (SCD Type 2), Change Data Capture (CDC), Change Quarantine Isolation Gates

---

## 🚀 Key Engineering Highlights & Performance Optimizations

### 1. Advanced SCD Type 2 Ingestion
Avoided expensive full-table scans by structuring an atomic `MERGE` operation. The staging pipeline utilizes a **Split-Row Pattern via a custom `UNION ALL` subquery**, creating structural flags to simultaneously deactivate old record histories and insert current variations in a single pass.

### 2. High-Performance Fingerprint Delta Tracking
Leveraged BigQuery's native `FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(...)))` engine to evaluate entire data rows as highly compact hashes. This isolates modified data records significantly faster than classic line-by-line column string comparisons.

### 3. Smart Compute Lifecycle Cost Isolation
To eliminate idle-compute cloud billing overhead, the Airflow pipeline deploys an ephemeral Dataproc cluster that explicitly forks ingestion scripts to run **completely in parallel**. Built-in `trigger_rule="all_done"` configurations guarantee that the cluster automatically tears down at completion, even if upstream extraction tasks experience mid-run failures.

### 4. Scheduler Optimization
Prevented Airflow cluster latency by completely abstracting local disk reading tasks away from top-level script parsing scopes. Implemented native Jinja templating configurations via `template_searchpath` to guarantee that file operations are executed only at runtime.

---
