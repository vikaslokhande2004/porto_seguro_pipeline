````markdown
# Porto Seguro Insurance Dimensional Analytics Pipeline

An end-to-end data engineering pipeline for transforming raw insurance claim data into a dimensional analytics warehouse using **Python, PySpark, BigQuery, Dataform, dbt, and Apache Airflow**.

The project demonstrates modern data engineering practices including data ingestion, data quality validation, schema handling, Bronze/Silver/Gold architecture, dimensional modeling, Slowly Changing Dimensions (SCD Type 2), ELT transformations, automated testing, analytics, and workflow orchestration.

---

## 📌 Project Overview

Insurance companies process large volumes of structured and semi-structured data related to:

- Policyholders
- Vehicles
- Insurance claims
- Claim amounts
- Accident information
- Fraud indicators
- Policyholder attributes

This project builds a complete analytics pipeline that transforms raw insurance data into a structured dimensional model optimized for analytical queries.

The pipeline follows a layered architecture:

```text
Raw Data
    │
    ▼
Bronze Layer
    │
    ▼
PySpark Transformation
    │
    ▼
Silver Layer
    │
    ▼
Dimensional Modeling
    │
    ├── Dimension Tables
    └── Fact Tables
    │
    ▼
BigQuery
    │
    ├── Dataform
    └── dbt
    │
    ▼
Gold Analytics Layer
    │
    ▼
Apache Airflow
````

---

# 🏗️ Architecture

```text
                  ┌─────────────────────┐
                  │    Raw Insurance    │
                  │        Data         │
                  └──────────┬──────────┘
                             │
                             ▼
                  ┌─────────────────────┐
                  │    Bronze Layer     │
                  │   Raw / Ingested    │
                  └──────────┬──────────┘
                             │
                             ▼
                  ┌─────────────────────┐
                  │      PySpark        │
                  │ Transformation Layer│
                  └──────────┬──────────┘
                             │
                             ▼
                  ┌─────────────────────┐
                  │    Silver Layer     │
                  │ Cleaned & Validated │
                  └──────────┬──────────┘
                             │
                             ▼
          ┌──────────────────────────────────────┐
          │       Dimensional Data Model         │
          │                                      │
          │  ┌───────────────┐                   │
          │  │ dim_date      │                   │
          │  └───────────────┘                   │
          │                                      │
          │  ┌───────────────┐                   │
          │  │ dim_vehicle   │                   │
          │  └───────────────┘                   │
          │                                      │
          │  ┌─────────────────────┐             │
          │  │ dim_policyholder    │             │
          │  │       (SCD2)        │             │
          │  └─────────────────────┘             │
          │                                      │
          │  ┌─────────────────────┐             │
          │  │    fact_claims      │             │
          │  └─────────────────────┘             │
          └──────────────────┬───────────────────┘
                             │
                             ▼
                  ┌─────────────────────┐
                  │      BigQuery       │
                  │   Data Warehouse    │
                  └──────────┬──────────┘
                             │
                  ┌──────────┴──────────┐
                  │                     │
                  ▼                     ▼
          ┌───────────────┐     ┌───────────────┐
          │   Dataform    │     │      dbt      │
          │ Transformations│    │ Run + Testing │
          └───────┬───────┘     └───────┬───────┘
                  │                     │
                  └──────────┬──────────┘
                             │
                             ▼
                  ┌─────────────────────┐
                  │    Gold Analytics   │
                  │ SQL Reports & KPIs  │
                  └─────────────────────┘
                             ▲
                             │
                  ┌─────────────────────┐
                  │   Apache Airflow    │
                  │   Orchestration     │
                  └─────────────────────┘
```

---

# 🎯 Project Objectives

The main objectives of this project are:

* Build an end-to-end insurance data pipeline.
* Process raw insurance claim data using PySpark.
* Implement a Bronze/Silver/Gold architecture.
* Perform data cleaning and transformation.
* Handle schema inconsistencies and data quality issues.
* Build a dimensional data warehouse.
* Implement SCD Type 2 for policyholder history.
* Create fact and dimension tables.
* Store analytical data in Google BigQuery.
* Use Dataform for SQL-based transformations.
* Use dbt for transformation management and data testing.
* Orchestrate pipeline tasks using Apache Airflow.
* Create analytical SQL reports using window functions.
* Maintain the project using Git and GitHub.

---

# 🛠️ Technology Stack

| Technology      | Purpose                                      |
| --------------- | -------------------------------------------- |
| Python          | Data processing and pipeline logic           |
| PySpark         | Large-scale data transformation              |
| Pandas          | Data analysis and preprocessing              |
| SQL             | Data transformation and analytics            |
| Google BigQuery | Cloud data warehouse                         |
| Google Cloud    | Cloud infrastructure                         |
| Dataform        | SQL transformation management                |
| dbt             | Data transformation and data quality testing |
| Apache Airflow  | Workflow orchestration                       |
| Git             | Version control                              |
| GitHub          | Source code management                       |
| WSL2            | Local Linux development environment          |

---

# 📂 Project Structure

```text
porto_seguro_pipeline/
│
├── dags/
│   └── porto_pipeline_dag.py
│
├── scripts/
│   ├── run_pipeline.sh
│   └── run_porto_pipeline.sh
│
├── src/
│   ├── ingestion/
│   │
│   ├── models/
│   │   ├── snapshot_builder.py
│   │   ├── scd2_builder.py
│   │   ├── dimension_builder.py
│   │   ├── date_dimension.py
│   │   └── fact_builder.py
│   │
│   ├── transform/
│   │   └── spark_porto.py
│   │
│   └── utils/
│
├── sql/
│   ├── analytics/
│   │
│   ├── dataform/
│   │
│   ├── dbt/
│   │   ├── models/
│   │   │   ├── staging/
│   │   │   │   ├── stg_claims.sql
│   │   │   │   └── schema.yml
│   │   │   │
│   │   │   └── marts/
│   │   │       ├── fact_claims.sql
│   │   │       └── schema.yml
│   │   │
│   │   └── dbt_project.yml
│   │
│   └── dimensional_sql/
│
├── raw/
│
├── silver/
│
├── processed/
│
├── reports/
│
├── logs/
│
├── notebooks/
│
├── requirements.txt
│
└── README.md
```

---

# 🔄 Data Pipeline Flow

The pipeline processes data through multiple stages.

## 1. Data Ingestion

Raw insurance data is collected and stored in the raw layer.

```text
Raw Dataset
     │
     ▼
raw/
```

The raw data is preserved as the source of truth.

---

## 2. Bronze Layer

The Bronze layer contains the initial ingested data with minimal transformation.

Purpose:

* Preserve original data.
* Maintain source-level information.
* Enable reproducibility.
* Support downstream transformations.

---

## 3. PySpark Transformation

PySpark processes the raw data and performs:

* Data type conversion
* Column normalization
* Null handling
* Schema validation
* Data quality checks
* Duplicate handling
* Transformation logic

The main transformation script is:

```text
src/transform/spark_porto.py
```

---

## 4. Silver Layer

The Silver layer contains cleaned and standardized insurance data.

```text
Bronze
   │
   ▼
PySpark
   │
   ▼
Silver
```

The Silver layer is used as the foundation for dimensional modeling and analytics.

---

# ⭐ Dimensional Data Model

The project uses a Star Schema design.

```text
                 ┌───────────────────┐
                 │    dim_date       │
                 └─────────┬─────────┘
                           │
                           │
┌───────────────────┐      │      ┌───────────────────┐
│ dim_policyholder  │──────┼──────│   dim_vehicle     │
│       SCD2         │      │      └───────────────────┘
└─────────┬─────────┘      │
          │                │
          │       ┌────────▼────────┐
          └──────►│   fact_claims   │
                  └─────────────────┘
```

## Fact Table

### `fact_claims`

Contains measurable insurance claim information.

Example metrics:

* Claim amount
* Fraud indicator
* Claim count
* Incident date
* Policyholder key
* Vehicle key

---

## Dimension Tables

### `dim_date`

Provides calendar-based analytics.

Example attributes:

* Date key
* Full date
* Year
* Month
* Day
* Quarter

---

### `dim_policyholder`

Contains policyholder attributes.

Example attributes:

* Policyholder key
* Occupation
* Relationship
* Demographic attributes
* Valid from
* Valid to

This dimension supports historical tracking using **SCD Type 2**.

---

### `dim_vehicle`

Contains vehicle-related information.

Example attributes:

* Vehicle key
* Auto make
* Auto year
* Vehicle characteristics

---

# 🔁 Slowly Changing Dimension Type 2

SCD Type 2 is implemented for policyholder data.

Historical changes are preserved using:

```text
valid_from
valid_to
```

Example:

```text
Policyholder
     │
     ├── Version 1
     │   valid_from = 2025-01-01
     │   valid_to   = 2025-12-31
     │
     └── Version 2
         valid_from = 2026-01-01
         valid_to   = NULL
```

This allows the system to answer point-in-time questions such as:

> What was the policyholder's occupation when the claim occurred?

---

# ☁️ Google BigQuery

BigQuery is used as the cloud data warehouse.

The project uses Google Cloud resources to store and analyze transformed insurance data.

The dimensional model is designed for analytical workloads and supports:

* Aggregations
* Reporting
* Historical analysis
* Fraud analysis
* Claims analytics
* Vehicle risk analysis

---

# 🧩 Dataform

Dataform is used within Google Cloud for SQL-based data transformation and warehouse management.

Dataform is managed through the Google Cloud environment rather than running the local Dataform CLI as part of the Airflow pipeline.

---

# 🧪 dbt

dbt is used for SQL transformation and data quality testing.

The dbt project is located at:

```text
sql/dbt/
```

Run dbt models:

```bash
cd sql/dbt
dbt run
```

Run data quality tests:

```bash
dbt test
```

The pipeline executes:

```bash
dbt run && dbt test
```

This ensures that the pipeline only succeeds when the transformation and data-quality tests pass.

---

# 📊 Gold Layer Analytics

The Gold layer provides business-ready analytical queries.

Example analytics include:

## Claims by Policyholder Occupation

Analyze:

* Number of claims
* Total claim amount
* Average claim amount
* Fraud flags
* Payout ranking

---

## Vehicle Risk Profile

Analyze:

* Claims by vehicle make
* Claims by vehicle year
* Average claim amount
* Vehicle risk ranking

---

## Monthly Claims Trend

Analyze:

* Monthly claim volume
* Monthly total payouts
* Previous month's payout
* Month-over-month changes

Window functions such as:

```sql
RANK()
DENSE_RANK()
LAG()
```

are used for analytical reporting.

---

# ⚙️ Apache Airflow

Apache Airflow is used to orchestrate the pipeline.

The DAG is:

```text
dags/porto_pipeline_dag.py
```

The current automated pipeline flow is:

```text
Spark Bronze → Silver
        │
        ▼
     dbt run
        │
        ▼
    dbt test
```

The Airflow dependency is:

```python
task_spark_silver >> task_dbt_test
```

The Spark task executes:

```bash
python -m src.transform.spark_porto
```

The dbt task executes:

```bash
cd sql/dbt
dbt run
dbt test
```

---

# 🚀 Running the Pipeline

## Step 1: Clone the Repository

```bash
git clone <YOUR_GITHUB_REPOSITORY_URL>
```

```bash
cd porto_seguro_pipeline
```

---

## Step 2: Create Virtual Environment

```bash
python -m venv venv
```

Activate the environment:

### Linux / WSL

```bash
source venv/bin/activate
```

### Windows

```powershell
venv\Scripts\activate
```

---

## Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

---

## Step 4: Run PySpark Transformation

```bash
python -m src.transform.spark_porto
```

---

## Step 5: Run dbt

```bash
cd sql/dbt
```

Run models:

```bash
dbt run
```

Run tests:

```bash
dbt test
```

---

## Step 6: Run the Pipeline Script

From the project root:

```bash
./scripts/run_porto_pipeline.sh
```

The pipeline performs:

```text
Spark Transformation
        │
        ▼
dbt Run
        │
        ▼
dbt Test
```

---

# 🌬️ Running with Airflow

Start the Airflow scheduler:

```bash
airflow scheduler
```

Start the Airflow webserver:

```bash
airflow webserver --port 8080
```

Open:

```text
http://localhost:8080
```

Check the DAG:

```bash
airflow dags list
```

Check import errors:

```bash
airflow dags list-import-errors
```

Test the DAG:

```bash
airflow dags test porto_pipeline 2026-07-24
```

Trigger the DAG:

```bash
airflow dags trigger porto_pipeline
```

The DAG ID is:

```text
porto_pipeline
```

---

# 🔐 Environment Variables

Sensitive credentials should never be committed to GitHub.

Use environment variables or a `.env` file locally.

Example:

```text
GOOGLE_CLOUD_PROJECT=your-project-id
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

Add sensitive files to `.gitignore`:

```text
.env
*.env
*.json
credentials/
secrets/
```

Never commit:

* API keys
* Service account keys
* Passwords
* Access tokens
* Private credentials

---

# 📈 Data Quality

The pipeline includes multiple data quality practices:

* Null validation
* Duplicate detection
* Schema validation
* Data type validation
* dbt tests
* Primary key uniqueness checks
* Not-null checks
* Historical dimension validation

dbt tests are executed automatically as part of the pipeline.

---

# 📚 Key Learning Outcomes

This project demonstrates practical experience with:

* End-to-end data engineering
* PySpark
* Data transformation
* Data quality
* Dimensional modeling
* Star schema design
* Fact and dimension tables
* SCD Type 2
* BigQuery
* Dataform
* dbt
* dbt testing
* SQL analytics
* Window functions
* Apache Airflow
* Pipeline automation
* Git and GitHub
* Cloud data warehousing

---

# 🔮 Future Improvements

Potential future enhancements include:

* Integrate Airflow directly with Google Cloud Dataform workflow execution.
* Add automated CI/CD using GitHub Actions.
* Add BigQuery cost monitoring.
* Add Cloud Composer deployment.
* Add automated data-quality alerts.
* Add monitoring and logging dashboards.
* Add Looker Studio or Power BI dashboards.
* Add incremental dbt models.
* Add partitioning and clustering to BigQuery tables.
* Add LLM-based adjuster note extraction as an optional pipeline stage.
* Add automated pipeline notifications through email or Slack.

---

# 👨‍💻 Author

**Vikas**

Data Engineering Project

Technologies:

```text
Python | PySpark | SQL | BigQuery | Dataform | dbt | Airflow | Git | GitHub
```
