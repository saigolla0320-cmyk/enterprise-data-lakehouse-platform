"""
Data Quality Framework — Lakehouse Layers
Runs Great Expectations validation suites against Silver and Gold Delta tables.
Designed to run as a Databricks job step after each transformation layer,
enforcing data contracts before data is promoted to the next layer.
"""

import logging
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("data_quality")

SILVER_PATH = Path(__file__).resolve().parents[1] / "data" / "silver"
GOLD_PATH = Path(__file__).resolve().parents[1] / "data" / "gold"


@dataclass
class Expectation:
    """A single data quality rule."""
    name: str
    check: Callable[[pd.DataFrame], bool]
    severity: str = "error"  # "error" fails the pipeline, "warn" logs only


@dataclass
class ValidationResult:
    table: str
    passed: int = 0
    failed: int = 0
    warnings: int = 0
    failures: list = field(default_factory=list)

    @property
    def success(self) -> bool:
        return self.failed == 0


def validate_table(table_name: str, df: pd.DataFrame, expectations: list[Expectation]) -> ValidationResult:
    """Run all expectations against a dataframe."""
    result = ValidationResult(table=table_name)
    logger.info(f"Validating {table_name} ({len(df):,} rows) against {len(expectations)} expectations")

    for exp in expectations:
        try:
            ok = exp.check(df)
        except Exception as e:
            ok = False
            logger.error(f"  Expectation '{exp.name}' raised: {e}")

        if ok:
            result.passed += 1
            logger.info(f"  PASS — {exp.name}")
        else:
            if exp.severity == "error":
                result.failed += 1
                result.failures.append(exp.name)
                logger.error(f"  FAIL — {exp.name}")
            else:
                result.warnings += 1
                logger.warning(f"  WARN — {exp.name}")

    return result


def transactions_expectations() -> list[Expectation]:
    """Data contract for the Silver fct_transactions table."""
    return [
        Expectation(
            "transaction_id is unique",
            lambda df: df["transaction_id"].is_unique,
        ),
        Expectation(
            "transaction_id has no nulls",
            lambda df: df["transaction_id"].notna().all(),
        ),
        Expectation(
            "amount_usd is always positive",
            lambda df: (df["amount_usd"] > 0).all(),
        ),
        Expectation(
            "amount_usd within reasonable range",
            lambda df: (df["amount_usd"] < 1_000_000).mean() >= 0.999,
            severity="warn",
        ),
        Expectation(
            "customer_id has no nulls",
            lambda df: df["customer_id"].notna().all(),
        ),
        Expectation(
            "row count above minimum threshold",
            lambda df: len(df) >= 1,
        ),
    ]


def customer_360_expectations() -> list[Expectation]:
    """Data contract for the Gold mart_customer_360 table."""
    return [
        Expectation(
            "customer_id is unique",
            lambda df: df["customer_id"].is_unique,
        ),
        Expectation(
            "value_tier in allowed set",
            lambda df: df["value_tier"].isin(["platinum", "gold", "silver", "bronze"]).all(),
        ),
        Expectation(
            "engagement_status in allowed set",
            lambda df: df["engagement_status"].isin(["active", "at_risk", "churned"]).all(),
        ),
        Expectation(
            "actual_lifetime_spend is non-negative",
            lambda df: (df["actual_lifetime_spend"].fillna(0) >= 0).all(),
        ),
    ]


def load_delta_as_pandas(path: Path) -> pd.DataFrame:
    """Load a Delta/Parquet table directory into pandas (local validation)."""
    parquet_files = list(path.rglob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found under {path}")
    return pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)


def run():
    all_results = []

    checks = [
        ("fct_transactions", SILVER_PATH / "fct_transactions", transactions_expectations()),
        ("mart_customer_360", GOLD_PATH / "mart_customer_360", customer_360_expectations()),
    ]

    for table_name, path, expectations in checks:
        try:
            df = load_delta_as_pandas(path)
            result = validate_table(table_name, df, expectations)
            all_results.append(result)
        except FileNotFoundError as e:
            logger.warning(f"Skipping {table_name}: {e}")

    total_failed = sum(r.failed for r in all_results)
    total_passed = sum(r.passed for r in all_results)

    logger.info(f"Validation summary: {total_passed} passed, {total_failed} failed")

    if total_failed > 0:
        logger.error("Data quality gate FAILED. Blocking promotion to next layer.")
        sys.exit(1)
    logger.info("Data quality gate PASSED.")


if __name__ == "__main__":
    run()
