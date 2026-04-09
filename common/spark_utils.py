from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def get_spark_session(app_name: str = "cloud_stock_analytics") -> SparkSession:
    """Return the active Databricks Spark session or create a local one."""
    return SparkSession.builder.appName(app_name).getOrCreate()


def write_delta_table(
    df: DataFrame,
    table_name: str,
    storage_path: str,
    mode: str = "overwrite",
) -> None:
    (
        df.write.format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .option("path", storage_path)
        .saveAsTable(table_name)
    )
