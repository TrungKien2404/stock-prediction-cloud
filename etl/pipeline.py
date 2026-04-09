from __future__ import annotations

import argparse
import logging

from pyspark.sql import functions as F

from common.config import load_config
from common.logging_utils import configure_logging
from common.path_utils import to_spark_path
from common.spark_utils import get_spark_session, write_delta_table
from etl.transform import add_technical_features, clean_raw_prices


LOGGER = logging.getLogger(__name__)


def run_etl_pipeline(config_path: str = "configs/settings.yaml", base_path: str | None = None) -> dict[str, str | int]:
    config = load_config(config_path, base_path)
    spark = get_spark_session("stock_etl_pipeline")

    raw_path = to_spark_path(config.resolve_path("raw_parquet_path"))
    processed_parquet_path = to_spark_path(config.resolve_path("processed_parquet_path"))
    delta_feature_path = to_spark_path(config.resolve_path("delta_feature_path"))

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {config.storage['delta_database']}")

    raw_df = spark.read.parquet(raw_path)
    feature_df = add_technical_features(
        clean_raw_prices(raw_df),
        ma_windows=config.features["ma_windows"],
        lag_days=config.features["lag_days"],
        rsi_window=config.features["rsi_window"],
    )

    (
        feature_df.write.mode("overwrite")
        .partitionBy("ticker")
        .parquet(processed_parquet_path)
    )

    feature_table_name = f"{config.storage['delta_database']}.{config.storage['delta_feature_table']}"
    write_delta_table(
        df=feature_df,
        table_name=feature_table_name,
        storage_path=delta_feature_path,
        mode="overwrite",
    )

    summary = feature_df.agg(
        F.count("*").alias("record_count"),
        F.min("trade_date").alias("min_trade_date"),
        F.max("trade_date").alias("max_trade_date"),
    ).collect()[0]

    LOGGER.info("ETL completed and Delta table refreshed: %s", feature_table_name)
    return {
        "feature_table": feature_table_name,
        "record_count": int(summary["record_count"]),
        "processed_parquet_path": processed_parquet_path,
        "delta_feature_path": delta_feature_path,
        "min_trade_date": str(summary["min_trade_date"]),
        "max_trade_date": str(summary["max_trade_date"]),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Spark ETL pipeline.")
    parser.add_argument("--config", default="configs/settings.yaml")
    parser.add_argument("--base-path", default=None)
    return parser.parse_args()


def main() -> None:
    configure_logging()
    args = parse_args()
    summary = run_etl_pipeline(config_path=args.config, base_path=args.base_path)
    LOGGER.info("ETL summary: %s", summary)


if __name__ == "__main__":
    main()
