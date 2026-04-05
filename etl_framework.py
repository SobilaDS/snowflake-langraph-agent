"""
Snowflake ETL Framework
=======================
Configuration-driven ETL pipeline framework powering data pipelines
across 20,000+ Snowflake tables .

Features:
  - YAML-config-driven pipeline definitions (zero boilerplate per pipeline)
  - COPY INTO bulk loading from S3 with parallel execution
  - MERGE-based idempotent upserts (no duplicates on retry)
  - Schema validation and drift detection at ingestion
  - Automated data quality assertions
  - SLA monitoring with Slack alerting
  - Full pipeline audit trail in Snowflake

Author : Sobila S
Stack  : Python · Snowflake · AWS S3 · Apache Airflow
"""

from __future__ import annotations

import os
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

import yaml
import boto3
import snowflake.connector
from snowflake.connector import DictCursor

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# CONFIG MODEL
# ─────────────────────────────────────────────

@dataclass
class QualityCheck:
    name:      str
    sql:       str            # SQL that must return 0 rows to pass
    severity:  str = "error"  # "error" | "warning"


@dataclass
class PipelineConfig:
    name:            str
    source_s3_path:  str          # s3://bucket/prefix/
    target_table:    str          # SCHEMA.TABLE_NAME
    staging_table:   str          # SCHEMA.TABLE_NAME_STAGE (transient)
    merge_key:       str          # Primary key column(s) for MERGE
    file_format:     str = "PARQUET"
    schedule:        str = "0 2 * * *"
    quality_checks:  list[QualityCheck] = field(default_factory=list)
    sla_minutes:     int  = 60
    owner:           str  = "sobila"
    tags:            list[str] = field(default_factory=list)

    @classmethod
    def from_yaml(cls, path: str) -> "PipelineConfig":
        with open(path) as f:
            raw = yaml.safe_load(f)
        qc = [QualityCheck(**c) for c in raw.pop("quality_checks", [])]
        return cls(**raw, quality_checks=qc)


# ─────────────────────────────────────────────
# SNOWFLAKE CLIENT
# ─────────────────────────────────────────────

class SnowflakeClient:
    def __init__(self):
        self.conn = snowflake.connector.connect(
            account   = os.environ["SNOWFLAKE_ACCOUNT"],
            user      = os.environ["SNOWFLAKE_USER"],
            password  = os.environ["SNOWFLAKE_PASSWORD"],
            warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            database  = os.environ.get("SNOWFLAKE_DATABASE", "ANALYTICS"),
        )

    def execute(self, sql: str, params: tuple = ()) -> list[dict]:
        cur = self.conn.cursor(DictCursor)
        cur.execute(sql, params)
        return cur.fetchall() or []

    def execute_many(self, sqls: list[str]):
        cur = self.conn.cursor()
        for sql in sqls:
            log.debug("SQL: %s", sql[:120])
            cur.execute(sql)
        self.conn.commit()

    def close(self):
        self.conn.close()


# ─────────────────────────────────────────────
# SCHEMA REGISTRY
# ─────────────────────────────────────────────

class SchemaRegistry:
    """Tracks registered table schemas for drift detection."""

    def __init__(self, sf: SnowflakeClient):
        self.sf = sf
        self._ensure_registry_table()

    def _ensure_registry_table(self):
        self.sf.execute("""
            CREATE TABLE IF NOT EXISTS etl_framework.schema_registry (
                table_name   VARCHAR,
                column_name  VARCHAR,
                data_type    VARCHAR,
                registered_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)

    def get_registered_schema(self, table: str) -> dict[str, str]:
        rows = self.sf.execute(
            "SELECT column_name, data_type FROM etl_framework.schema_registry WHERE table_name = %s",
            (table,)
        )
        return {r["COLUMN_NAME"]: r["DATA_TYPE"] for r in rows}

    def get_live_schema(self, table: str) -> dict[str, str]:
        schema, tbl = table.split(".")
        rows = self.sf.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = '{schema.upper()}' AND table_name = '{tbl.upper()}'
        """)
        return {r["COLUMN_NAME"]: r["DATA_TYPE"] for r in rows}

    def diff(self, table: str) -> dict:
        registered = self.get_registered_schema(table)
        live       = self.get_live_schema(table)
        return {
            "new_columns":     list(set(live) - set(registered)),
            "removed_columns": list(set(registered) - set(live)),
            "type_changes":    {c: (registered[c], live[c]) for c in live
                                if c in registered and live[c] != registered[c]},
        }

    def register(self, table: str):
        live = self.get_live_schema(table)
        self.sf.execute(f"DELETE FROM etl_framework.schema_registry WHERE table_name = '{table}'")
        for col, dtype in live.items():
            self.sf.execute(
                "INSERT INTO etl_framework.schema_registry (table_name, column_name, data_type) VALUES (%s, %s, %s)",
                (table, col, dtype)
            )


# ─────────────────────────────────────────────
# PIPELINE RUNNER
# ─────────────────────────────────────────────

class PipelineRunner:
    def __init__(self, config: PipelineConfig):
        self.cfg      = config
        self.sf       = SnowflakeClient()
        self.registry = SchemaRegistry(self.sf)
        self.run_id   = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        self.start_ts = time.time()

    # ── Step 1: Schema drift check ─────────────

    def check_schema_drift(self):
        log.info("[%s] Checking schema drift...", self.cfg.name)
        diff = self.registry.diff(self.cfg.target_table)
        if diff["removed_columns"]:
            log.warning("Removed columns detected: %s", diff["removed_columns"])
            self._alert(f"⚠️ Schema drift: removed columns {diff['removed_columns']} in {self.cfg.target_table}", "warning")
        if diff["new_columns"]:
            log.info("New columns detected — will auto-add: %s", diff["new_columns"])
            for col in diff["new_columns"]:
                self.sf.execute(f"ALTER TABLE {self.cfg.target_table} ADD COLUMN IF NOT EXISTS {col} VARCHAR")
        return diff

    # ── Step 2: Create staging table ───────────

    def create_staging_table(self):
        log.info("[%s] Creating transient staging table...", self.cfg.name)
        self.sf.execute_many([
            f"DROP TABLE IF EXISTS {self.cfg.staging_table}",
            f"""
            CREATE TRANSIENT TABLE {self.cfg.staging_table}
            LIKE {self.cfg.target_table}
            """,
        ])

    # ── Step 3: COPY INTO from S3 ──────────────

    def copy_from_s3(self) -> int:
        log.info("[%s] COPY INTO from S3: %s", self.cfg.name, self.cfg.source_s3_path)
        s3_integration = os.environ.get("SNOWFLAKE_S3_INTEGRATION", "S3_INTEGRATION")
        sql = f"""
            COPY INTO {self.cfg.staging_table}
            FROM '{self.cfg.source_s3_path}'
            STORAGE_INTEGRATION = {s3_integration}
            FILE_FORMAT = (TYPE = {self.cfg.file_format})
            ON_ERROR = 'ABORT_STATEMENT'
            PURGE = FALSE
            PARALLEL = 8
        """
        rows = self.sf.execute(sql)
        loaded = sum(r.get("ROWS_LOADED", 0) for r in rows)
        log.info("[%s] Loaded %d rows into staging", self.cfg.name, loaded)
        return loaded

    # ── Step 4: Row count validation ───────────

    def validate_row_counts(self, staged_rows: int):
        log.info("[%s] Validating row counts...", self.cfg.name)
        if staged_rows == 0:
            raise ValueError(f"Pipeline {self.cfg.name}: zero rows loaded from S3 — aborting.")
        prev_rows = self.sf.execute(f"SELECT COUNT(*) AS cnt FROM {self.cfg.target_table}")[0]["CNT"]
        if prev_rows > 0:
            ratio = staged_rows / prev_rows
            if ratio < 0.5:
                self._alert(
                    f"🚨 Row count anomaly: {self.cfg.name} loaded {staged_rows} rows "
                    f"vs {prev_rows} existing ({ratio:.0%}). Investigate before merge.",
                    "critical"
                )
                raise ValueError(f"Row count ratio {ratio:.2f} below 0.5 threshold — aborting merge.")

    # ── Step 5: MERGE (idempotent upsert) ──────

    def merge_to_target(self):
        log.info("[%s] Merging staging → target...", self.cfg.name)
        # Get column list dynamically
        cols = list(self.registry.get_live_schema(self.cfg.target_table).keys())
        merge_key = self.cfg.merge_key
        col_list  = ", ".join(cols)
        set_clause = ", ".join(f"t.{c} = s.{c}" for c in cols if c != merge_key)
        val_list  = ", ".join(f"s.{c}" for c in cols)

        sql = f"""
            MERGE INTO {self.cfg.target_table} AS t
            USING {self.cfg.staging_table}     AS s
            ON t.{merge_key} = s.{merge_key}
            WHEN MATCHED THEN
                UPDATE SET {set_clause},
                           t._updated_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
                INSERT ({col_list}, _loaded_at)
                VALUES ({val_list}, CURRENT_TIMESTAMP())
        """
        self.sf.execute(sql)
        log.info("[%s] Merge complete.", self.cfg.name)

    # ── Step 6: Quality checks ─────────────────

    def run_quality_checks(self):
        log.info("[%s] Running %d quality checks...", self.cfg.name, len(self.cfg.quality_checks))
        failures = []
        for qc in self.cfg.quality_checks:
            rows = self.sf.execute(qc.sql)
            if rows:
                msg = f"Quality check FAILED: {qc.name} — {len(rows)} violating rows"
                log.error(msg)
                failures.append({"check": qc.name, "rows": len(rows)})
                if qc.severity == "error":
                    self._alert(f"🚨 {msg}", "critical")
                else:
                    self._alert(f"⚠️ {msg}", "warning")
        return failures

    # ── Step 7: SLA check ──────────────────────

    def check_sla(self):
        elapsed_minutes = (time.time() - self.start_ts) / 60
        if elapsed_minutes > self.cfg.sla_minutes:
            self._alert(
                f"⏱️ SLA breach: {self.cfg.name} took {elapsed_minutes:.1f}m "
                f"(SLA: {self.cfg.sla_minutes}m)",
                "warning"
            )

    # ── Step 8: Audit log ──────────────────────

    def write_audit_log(self, staged_rows: int, qc_failures: list):
        elapsed = round(time.time() - self.start_ts, 1)
        self.sf.execute("""
            INSERT INTO etl_framework.pipeline_audit_log
                (run_id, pipeline_name, staged_rows, qc_failures, elapsed_seconds, completed_at)
            VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
        """, (self.run_id, self.cfg.name, staged_rows, json.dumps(qc_failures), elapsed))

    # ── Orchestrator ───────────────────────────

    def run(self) -> dict:
        log.info("=" * 60)
        log.info("Pipeline: %s  |  run_id: %s", self.cfg.name, self.run_id)
        log.info("=" * 60)
        try:
            self.check_schema_drift()
            self.create_staging_table()
            staged_rows = self.copy_from_s3()
            self.validate_row_counts(staged_rows)
            self.merge_to_target()
            qc_failures = self.run_quality_checks()
            self.check_sla()
            self.registry.register(self.cfg.target_table)
            self.write_audit_log(staged_rows, qc_failures)
            log.info("✅ Pipeline %s complete. Rows: %d  QC failures: %d",
                     self.cfg.name, staged_rows, len(qc_failures))
            return {"status": "success", "rows": staged_rows, "qc_failures": qc_failures}
        except Exception as e:
            log.exception("Pipeline %s FAILED: %s", self.cfg.name, e)
            self._alert(f"🚨 Pipeline FAILED: {self.cfg.name}\n{e}", "critical")
            return {"status": "failed", "error": str(e)}
        finally:
            self.sf.close()

    def _alert(self, message: str, severity: str = "warning"):
        import urllib.request
        webhook = os.environ.get("SLACK_WEBHOOK_URL")
        if not webhook:
            return
        payload = json.dumps({"text": message}).encode()
        req = urllib.request.Request(webhook, data=payload,
                                     headers={"Content-Type": "application/json"})
        try:
            urllib.request.urlopen(req, timeout=3)
        except Exception:
            pass


# ─────────────────────────────────────────────
# DAG FACTORY  (generates Airflow DAGs from configs)
# ─────────────────────────────────────────────

def create_dag_for_pipeline(config_path: str):
    """
    DAG factory function.
    Call from an Airflow DAGs file to auto-generate a DAG for each config.

    Usage in your Airflow DAGs folder:
        from etl_framework import create_dag_for_pipeline
        dag = create_dag_for_pipeline("configs/provider_features.yaml")
    """
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from datetime import timedelta

    cfg = PipelineConfig.from_yaml(config_path)

    default_args = {
        "owner":            cfg.owner,
        "retries":          2,
        "retry_delay":      timedelta(minutes=5),
        "email_on_failure": True,
    }

    with DAG(
        dag_id            = cfg.name,
        default_args      = default_args,
        schedule_interval = cfg.schedule,
        catchup           = False,
        tags              = cfg.tags + ["etl-framework"],
    ) as dag:

        def _run(**context):
            runner = PipelineRunner(cfg)
            result = runner.run()
            if result["status"] == "failed":
                raise RuntimeError(result["error"])
            context["ti"].xcom_push(key="rows_loaded", value=result["rows"])

        PythonOperator(task_id="run_pipeline", python_callable=_run, provide_context=True)

    return dag


# ─────────────────────────────────────────────
# CLI ENTRYPOINT
# ─────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    config_file = sys.argv[1] if len(sys.argv) > 1 else "configs/example.yaml"
    cfg    = PipelineConfig.from_yaml(config_file)
    runner = PipelineRunner(cfg)
    result = runner.run()
    print(json.dumps(result, indent=2))
