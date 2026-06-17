"""Unit tests for the data quality framework."""

import pandas as pd
import pytest

from quality.data_quality_framework import (
    Expectation,
    validate_table,
    transactions_expectations,
)


def test_passing_validation():
    df = pd.DataFrame({
        "transaction_id": ["t1", "t2", "t3"],
        "customer_id": ["c1", "c2", "c3"],
        "amount_usd": [10.0, 25.5, 100.0],
    })
    result = validate_table("test", df, transactions_expectations())
    assert result.success
    assert result.failed == 0


def test_failing_on_duplicate_ids():
    df = pd.DataFrame({
        "transaction_id": ["t1", "t1", "t3"],
        "customer_id": ["c1", "c2", "c3"],
        "amount_usd": [10.0, 25.5, 100.0],
    })
    result = validate_table("test", df, transactions_expectations())
    assert not result.success
    assert "transaction_id is unique" in result.failures


def test_failing_on_negative_amount():
    df = pd.DataFrame({
        "transaction_id": ["t1", "t2"],
        "customer_id": ["c1", "c2"],
        "amount_usd": [10.0, -5.0],
    })
    result = validate_table("test", df, transactions_expectations())
    assert not result.success


def test_custom_expectation():
    df = pd.DataFrame({"x": [1, 2, 3]})
    exp = Expectation("x sums to 6", lambda d: d["x"].sum() == 6)
    result = validate_table("test", df, [exp])
    assert result.success
