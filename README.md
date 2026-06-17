# Enterprise Data Lakehouse Platform

A cloud-native **data lakehouse** built on Azure, implementing the medallion architecture
(Bronze → Silver → Gold) with automated ingestion, PySpark/Delta transformations, DBT
modeling, Great Expectations data quality gates, Terraform-provisioned infrastructure,
and a full GitHub Actions CI/CD pipeline.

> Built to mirror enterprise-scale data platforms running on Azure Databricks, ADLS,
> DBT, and Snowflake — supporting analytics, reporting, and BI workloads.

---

## Highlights

- **Multi-source ingestion framework** — pluggable ingestors for REST APIs, databases, and flat files
- **Medallion architecture** — Bronze (raw) → Silver (cleaned, SCD Type 2) → Gold (business marts)
- **PySpark + Delta Lake** transformations as Databricks notebooks
- **DBT** modeling layer — staging views, ephemeral intermediates, materialized marts
- **Custom data quality framework** with per-table data contracts that gate layer promotion
- **Terraform** modules provisioning ADLS Gen2, Databricks workspace, and Key Vault
- **CI/CD** — lint, test, DBT build, Terraform validate, and Snowflake deploy on merge

---

## Architecture

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the full diagram and design rationale.

```
Sources → Bronze (raw Delta) → Silver (cleaned + validated) → Gold (marts → Snowflake)
              ▲                        ▲                            ▲
        Ingestion fw            Databricks PySpark              DBT models
                                Great Expectations gate
```

---

## Tech Stack

| Concern | Technology |
|---|---|
| Ingestion | Python (REST / DB / file ingestors) |
| Lake storage | Azure ADLS Gen2 (Delta Lake) |
| Compute | Azure Databricks (PySpark 3.5) |
| SQL transformation | DBT Core 1.7 |
| Data quality | Great Expectations + custom contract framework |
| Reporting warehouse | Snowflake |
| Infrastructure | Terraform (azurerm + databricks providers) |
| CI/CD | GitHub Actions |
| Local dev warehouse | DuckDB (mirrors Snowflake) |

---

## Project Structure

```
enterprise-data-lakehouse-platform/
├── ingestion/
│   └── multi_source_ingestor.py    # Bronze: REST / DB / file ingestion framework
├── databricks/
│   ├── 01_bronze_to_silver.py      # Silver: clean, dedupe, SCD Type 2
│   └── 02_silver_to_gold.py        # Gold: aggregations + Snowflake export
├── dbt_lakehouse/
│   ├── models/
│   │   ├── staging/                # Views over silver sources
│   │   ├── intermediate/           # Ephemeral enrichment models
│   │   └── marts/                  # mart_daily_revenue, mart_customer_360
│   ├── macros/                     # safe_divide, surrogate_key, grants
│   ├── dbt_project.yml
│   └── profiles.yml                # dev (DuckDB) + prod (Snowflake)
├── quality/
│   └── data_quality_framework.py   # Data contracts + layer-promotion gate
├── terraform/
│   ├── main.tf                     # ADLS, Databricks, Key Vault
│   ├── variables.tf
│   └── outputs.tf
├── .github/workflows/
│   └── ci-cd.yml                   # Lint → test → dbt → terraform → deploy
├── config/sources.json            # Declarative source definitions
├── docs/ARCHITECTURE.md
├── requirements.txt
├── .env.example
└── .gitignore
```

---

## Quick Start

### 1. Setup

```bash
git clone https://github.com/your-username/enterprise-data-lakehouse-platform.git
cd enterprise-data-lakehouse-platform

python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt

cp .env.example .env
```

### 2. Run the pipeline locally

```bash
# Bronze — ingest from configured sources
python ingestion/multi_source_ingestor.py

# Silver — clean & conform (requires Java for PySpark)
python databricks/01_bronze_to_silver.py

# Quality gate — validate silver
python quality/data_quality_framework.py

# Gold — Databricks aggregations
python databricks/02_silver_to_gold.py

# DBT — build the modeled gold layer
cd dbt_lakehouse
dbt deps --profiles-dir .
dbt build --profiles-dir .          # runs + tests
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir .
```

### 3. Provision Azure infrastructure

```bash
cd terraform
terraform init
terraform plan -var="tenant_id=$ARM_TENANT_ID" -var="environment=dev"
terraform apply
```

---

## Data Quality Contracts

Each table has an enforced data contract in `quality/data_quality_framework.py`:

| Table | Sample expectations |
|---|---|
| `fct_transactions` (silver) | `transaction_id` unique & non-null, `amount_usd > 0`, volume floor |
| `mart_customer_360` (gold) | `customer_id` unique, `value_tier` in allowed set, non-negative spend |

`error`-severity failures **block promotion** to the next layer; `warn`-severity failures
are logged but don't fail the pipeline. This gate runs between Silver and Gold.

---

## DBT Layer

- **Staging** (`stg_*`) — typed views over Silver Delta sources
- **Intermediate** (`int_*`) — ephemeral models joining facts and dimensions
- **Marts** (`mart_*`) — materialized tables:
  - `mart_daily_revenue` — revenue rollup by segment & region (Power BI)
  - `mart_customer_360` — CRM + behavior, value tiers, churn status

Reusable macros: `safe_divide`, `generate_surrogate_key`, `cents_to_dollars`,
`grant_select_to_reporting`.

---

## CI/CD Pipeline

On every push and PR, GitHub Actions runs:

1. **lint-and-test** — `ruff check` + `pytest` with coverage
2. **dbt-build** — `dbt build` against DuckDB (runs + tests all models)
3. **terraform-validate** — `terraform fmt -check` + `validate`
4. **deploy** — on merge to `main`, `dbt build --target prod` against Snowflake

Production secrets are injected from GitHub Actions secrets, never committed.

---

## Key Engineering Decisions

- **SCD Type 2 customer dimension** — preserves history for point-in-time analysis.
- **Ephemeral intermediate models** — keep the warehouse clean while reusing join logic.
- **Data contracts as code** — quality rules live next to the pipeline, version-controlled.
- **DuckDB / Snowflake parity** — same DBT models run locally and in production.
- **Terraform-managed infra** — dev/staging/prod are reproducible from one codebase.

---

## License

MIT
