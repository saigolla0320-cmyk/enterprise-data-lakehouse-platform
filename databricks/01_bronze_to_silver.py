# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze → Silver Transformation
# MAGIC
# MAGIC This notebook reads raw data from the Bronze layer (ADLS), applies cleaning,
# MAGIC deduplication, and standardization, then writes validated Delta tables to the
# MAGIC Silver layer. Designed to run on Azure Databricks with Unity Catalog.
# MAGIC
# MAGIC **Layer:** Silver
# MAGIC **Owner:** data-engineering
# MAGIC **Schedule:** Triggered by ADF after Bronze ingestion completes

# COMMAND ----------

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType, DoubleType, IntegerType, StringType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("silver_transform")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# In Databricks these would be widgets / job parameters
BRONZE_CATALOG = "lakehouse.bronze"
SILVER_CATALOG = "lakehouse.silver"

# Local fallback paths (when not running on Databricks)
BRONZE_PATH = "data/bronze"
SILVER_PATH = "data/silver"


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("bronze-to-silver")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformation: Customer Transactions

# COMMAND ----------


def transform_transactions(spark: SparkSession) -> DataFrame:
    """Clean and standardize customer transactions from bronze."""
    df = spark.read.format("delta").load(f"{BRONZE_PATH}/customer_transactions")

    df = (
        df
        .withColumn("transaction_id", F.col("transaction_id").cast(StringType()))
        .withColumn("customer_id", F.col("customer_id").cast(StringType()))
        .withColumn("amount", F.col("amount").cast(DoubleType()))
        .withColumn("transaction_ts", F.col("transaction_ts").cast(TimestampType()))
        .filter(F.col("amount") > 0)
        .filter(F.col("customer_id").isNotNull())
    )

    # Deduplicate — keep latest record per transaction_id
    window = Window.partitionBy("transaction_id").orderBy(F.col("_ingested_at").desc())
    df = (
        df
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    # Standardize and enrich
    df = (
        df
        .withColumn("amount_usd", F.round(F.col("amount"), 2))
        .withColumn("transaction_date", F.to_date("transaction_ts"))
        .withColumn("transaction_hour", F.hour("transaction_ts"))
        .withColumn("_processed_at", F.current_timestamp())
    )

    logger.info(f"Transactions silver row count: {df.count():,}")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformation: Customer Master (SCD Type 2)

# COMMAND ----------


def transform_customer_master(spark: SparkSession) -> DataFrame:
    """Build SCD Type 2 customer dimension from bronze customer_master."""
    df = spark.read.format("delta").load(f"{BRONZE_PATH}/customer_master")

    df = (
        df
        .withColumn("customer_id", F.col("customer_id").cast(StringType()))
        .withColumn("lifetime_value", F.col("lifetime_value").cast(DoubleType()))
        .withColumn("signup_date", F.to_date("signup_date"))
        .filter(F.col("customer_id").isNotNull())
        .dropDuplicates(["customer_id"])
    )

    # SCD Type 2 framing — effective dating
    df = (
        df
        .withColumn("effective_from", F.current_date())
        .withColumn("effective_to", F.lit("9999-12-31").cast("date"))
        .withColumn("is_current", F.lit(True))
        .withColumn("_processed_at", F.current_timestamp())
    )

    logger.info(f"Customer master silver row count: {df.count():,}")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver (Delta)

# COMMAND ----------


def write_silver(df: DataFrame, table_name: str, partition_col: str = None) -> None:
    """Write a dataframe to the silver layer as a Delta table."""
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_col:
        writer = writer.partitionBy(partition_col)
    writer.save(f"{SILVER_PATH}/{table_name}")
    logger.info(f"Wrote silver table: {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main

# COMMAND ----------


def main():
    spark = get_spark()

    transactions = transform_transactions(spark)
    write_silver(transactions, "fct_transactions", partition_col="transaction_date")

    customers = transform_customer_master(spark)
    write_silver(customers, "dim_customer")

    logger.info("Bronze → Silver transformation complete")


if __name__ == "__main__":
    main()
