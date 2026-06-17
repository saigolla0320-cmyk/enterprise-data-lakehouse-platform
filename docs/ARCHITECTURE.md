# Architecture

## Overview

This platform implements a **cloud-native data lakehouse** on Azure, following the
medallion architecture pattern. It unifies the reliability of a data warehouse with
the flexibility and scale of a data lake.

```
┌──────────────┐   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│   SOURCES    │   │   BRONZE     │   │   SILVER     │   │    GOLD      │
│              │   │              │   │              │   │              │
│ REST APIs    │──▶│ Raw, as-is   │──▶│ Cleaned,     │──▶│ Aggregated,  │
│ Databases    │   │ Delta tables │   │ deduplicated │   │ business-    │
│ Flat files   │   │ Partitioned  │   │ SCD Type 2   │   │ ready marts  │
│ Web events   │   │ by ingest_dt │   │ Validated    │   │ Snowflake    │
└──────────────┘   └──────────────┘   └──────────────┘   └──────────────┘
       │                  │                  │                  │
       │            ┌─────┴────────┐   ┌─────┴────────┐   ┌─────┴────────┐
       │            │ Multi-source │   │ Databricks   │   │ DBT models   │
       └───────────▶│ ingestion    │   │ PySpark +    │   │ Power BI     │
                    │ framework    │   │ Delta Lake   │   │ Snowflake    │
                    └──────────────┘   └──────────────┘   └──────────────┘
                                              │
                                       ┌──────┴───────┐
                                       │ Great        │
                                       │ Expectations │
                                       │ quality gate │
                                       └──────────────┘
```

## Layers

### Bronze — Raw landing zone
- Multi-source ingestion framework (`ingestion/multi_source_ingestor.py`) with pluggable
  ingestors for REST APIs, databases, and flat files.
- Data lands **as-is** with two metadata columns appended: `_ingested_at` and `_source_system`.
- Partitioned by `ingest_date` for efficient reprocessing.
- 90-day retention.

### Silver — Cleaned & conformed
- Databricks PySpark notebooks (`databricks/01_bronze_to_silver.py`).
- Type casting, null handling, deduplication (latest-record-wins via window functions).
- Customer dimension built as **SCD Type 2** with effective dating.
- Written as Delta tables, partitioned by business date.

### Gold — Business-ready
- Databricks aggregations (`databricks/02_silver_to_gold.py`) **and** DBT models
  (`dbt_lakehouse/`).
- DBT layer: staging views → ephemeral intermediates → materialized marts.
- Marts exported to Snowflake for the enterprise reporting layer.

## Data Quality

A custom data quality framework (`quality/data_quality_framework.py`) runs as a gate
between layers. Each table has a **data contract** — a set of expectations covering
uniqueness, completeness, ranges, and accepted values. Failures block promotion to the
next layer.

## Infrastructure

All Azure infrastructure is provisioned via Terraform (`terraform/`):
- ADLS Gen2 storage account with hierarchical namespace
- Bronze / Silver / Gold containers
- Databricks workspace (premium SKU in prod)
- Key Vault for secrets

## CI/CD

GitHub Actions (`.github/workflows/ci-cd.yml`) runs on every push and PR:
1. **Lint & test** — ruff + pytest
2. **DBT build** — runs and tests all models against DuckDB
3. **Terraform validate** — fmt check + validation
4. **Deploy** — on merge to main, runs DBT against Snowflake (production)

## Design Principles

- **Separation of concerns** — each layer has a single responsibility.
- **Idempotency** — all transformations are safely re-runnable.
- **Fail-fast quality gates** — bad data never reaches reporting.
- **Infrastructure as code** — environments are reproducible.
- **Environment parity** — DuckDB locally mirrors Snowflake in production.
