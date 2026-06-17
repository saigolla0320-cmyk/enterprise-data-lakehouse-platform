# Databricks notebook source
# MAGIC %md
# MAGIC # Silver → Gold Aggregation
# MAGIC
# MAGIC Reads validated Silver Delta tables and builds business-level Gold aggregates
# MAGIC for enterprise reporting and BI. Gold tables feed Power BI and Snowflake.
# MAGIC
# MAGIC **Layer:** Gold
# MAGIC **Owner:** analytics-engineering
# MAGIC **Consumers:** Power BI, Snowflake reporting layer

# COMMAND ----------

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gold_aggregation")

SILVER_PATH = "data/silver"
GOLD_PATH = "data/gold"


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("silver-to-gold")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: Daily Revenue by Customer Segment

# COMMAND ----------


def build_revenue_by_segment(spark: SparkSession) -> DataFrame:
    transactions = spark.read.format("delta").load(f"{SILVER_PATH}/fct_transactions")
    customers = spark.read.format("delta").load(f"{SILVER_PATH}/dim_customer")

    joined = transactions.join(
        customers.select("customer_id", "segment", "region"),
        on="customer_id",
        how="left",
    )

    agg = (
        joined
        .groupBy("transaction_date", "segment", "region")
        .agg(
            F.count("transaction_id").alias("transaction_count"),
            F.countDistinct("customer_id").alias("active_customers"),
            F.round(F.sum("amount_usd"), 2).alias("total_revenue"),
            F.round(F.avg("amount_usd"), 2).alias("avg_transaction_value"),
        )
        .withColumn("_processed_at", F.current_timestamp())
    )

    logger.info(f"Revenue by segment row count: {agg.count():,}")
    return agg

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: Customer Lifetime Value Summary

# COMMAND ----------


def build_customer_ltv(spark: SparkSession) -> DataFrame:
    transactions = spark.read.format("delta").load(f"{SILVER_PATH}/fct_transactions")
    customers = spark.read.format("delta").load(f"{SILVER_PATH}/dim_customer")

    txn_summary = (
        transactions
        .groupBy("customer_id")
        .agg(
            F.count("transaction_id").alias("lifetime_transactions"),
            F.round(F.sum("amount_usd"), 2).alias("lifetime_spend"),
            F.min("transaction_date").alias("first_transaction_date"),
            F.max("transaction_date").alias("last_transaction_date"),
        )
    )

    enriched = (
        customers
        .filter(F.col("is_current") == True)
        .join(txn_summary, on="customer_id", how="left")
        .withColumn(
            "tenure_days",
            F.datediff(F.current_date(), F.col("signup_date")),
        )
        .withColumn(
            "customer_tier",
            F.when(F.col("lifetime_spend") >= 10000, "platinum")
            .when(F.col("lifetime_spend") >= 5000, "gold")
            .when(F.col("lifetime_spend") >= 1000, "silver")
            .otherwise("bronze"),
        )
        .withColumn("_processed_at", F.current_timestamp())
    )

    logger.info(f"Customer LTV row count: {enriched.count():,}")
    return enriched

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Gold + Export to Snowflake

# COMMAND ----------


def write_gold(df: DataFrame, table_name: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{GOLD_PATH}/{table_name}")
    )
    logger.info(f"Wrote gold table: {table_name}")


def export_to_snowflake(df: DataFrame, table_name: str, sf_options: dict) -> None:
    """Push a gold table to Snowflake reporting schema.

    In production this uses the Spark-Snowflake connector. Configured via
    environment / secret scope, not hardcoded.
    """
    (
        df.write
        .format("snowflake")
        .options(**sf_options)
        .option("dbtable", table_name)
        .mode("overwrite")
        .save()
    )
    logger.info(f"Exported {table_name} to Snowflake")

# COMMAND ----------


def main():
    spark = get_spark()

    revenue = build_revenue_by_segment(spark)
    write_gold(revenue, "agg_revenue_by_segment")

    ltv = build_customer_ltv(spark)
    write_gold(ltv, "dim_customer_ltv")

    logger.info("Silver → Gold aggregation complete")


if __name__ == "__main__":
    main()
