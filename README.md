# 🏔️ Snowflake ETL Framework

> **Configuration-driven ETL pipeline framework — add a YAML file, get a production-ready Airflow DAG with quality checks, schema drift detection, idempotent MERGE, and SLA monitoring.**


---

## 🧠 What It Does

Managing 30+ ETL pipelines individually means 30× the boilerplate, 30× the maintenance, and 30× the potential for inconsistency. This framework reduces a new pipeline to a single YAML config file.

**Every pipeline gets — for free:**

| Feature | Details |
|---------|---------|
| 🔄 **Idempotent MERGE** | MERGE-based upserts — safe to retry, no duplicates |
| 📋 **Schema drift detection** | Detects added/removed/changed columns before each run |
| ⬇️ **Parallel S3 COPY INTO** | Bulk Snowflake loads with `PARALLEL = 8` |
| ✅ **Data quality assertions** | SQL-based checks run after every load |
| ⏱️ **SLA monitoring** | Alerts if pipeline exceeds defined SLA |
| 📊 **Audit trail** | Every run logged to `pipeline_audit_log` in Snowflake |
| 🔔 **Slack alerting** | Severity-tiered alerts for failures and warnings |
| 🏭 **DAG factory** | Auto-generates Airflow DAG from config |

---

## 🏗️ Pipeline Execution Flow

```
                    ┌──────────────────────────────────────┐
                    │         PipelineRunner.run()          │
                    └──────────────────┬───────────────────┘
                                       │
              ┌────────────────────────▼──────────────────────────┐
              │ 1. check_schema_drift()    → auto-ALTER if new cols│
              │ 2. create_staging_table()  → transient (cheap)     │
              │ 3. copy_from_s3()          → COPY INTO PARALLEL=8  │
              │ 4. validate_row_counts()   → abort if >50% drop    │
              │ 5. merge_to_target()       → idempotent MERGE       │
              │ 6. run_quality_checks()    → SQL assertions         │
              │ 7. check_sla()             → alert if over SLA      │
              │ 8. registry.register()     → update schema snapshot │
              │ 9. write_audit_log()       → Snowflake audit table  │
              └───────────────────────────────────────────────────┘
```

---

## 🚀 Quickstart

```bash
# 1. Clone
git clone https://github.com/SobilaDS/snowflake-etl-framework.git
cd snowflake-etl-framework

# 2. Install
pip install -r requirements.txt

# 3. Configure environment
cp .env.example .env

# 4. Create a pipeline config
cp configs/provider_features.yaml configs/my_pipeline.yaml
# Edit: name, source_s3_path, target_table, merge_key, quality_checks

# 5. Run the pipeline
python etl_framework.py configs/my_pipeline.yaml
```

---

## 📝 Adding a New Pipeline (30 seconds)

Create a YAML config:

```yaml
name:           my_new_pipeline
source_s3_path: s3://my-bucket/my-data/daily/
target_table:   ANALYTICS.PUBLIC.MY_TABLE
staging_table:  ANALYTICS.STAGING.MY_TABLE_STAGE
merge_key:      RECORD_ID
file_format:    PARQUET
schedule:       "0 3 * * *"
sla_minutes:    30
owner:          your.name

quality_checks:
  - name:     no_null_ids
    sql:      SELECT id FROM ANALYTICS.STAGING.MY_TABLE_STAGE WHERE id IS NULL
    severity: error
```

Then in your Airflow DAGs folder:

```python
from etl_framework import create_dag_for_pipeline
dag = create_dag_for_pipeline("configs/my_new_pipeline.yaml")
```

That's it. Full production pipeline with monitoring, quality checks, and alerting. ✅

---

## 📁 Project Structure

```
snowflake-etl-framework/
├── etl_framework.py          # Core framework — PipelineRunner + DAG factory
├── configs/
│   └── provider_features.yaml  # Example pipeline config
├── requirements.txt
├── .env.example
└── README.md
```



## 🛠️ Tech Stack

![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=flat-square&logo=snowflake&logoColor=white)
![AWS S3](https://img.shields.io/badge/AWS_S3-569A31?style=flat-square&logo=amazons3&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)
![YAML](https://img.shields.io/badge/YAML_Config-CC0000?style=flat-square&logo=yaml&logoColor=white)

---

## 📄 License

MIT
