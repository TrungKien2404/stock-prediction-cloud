from __future__ import annotations

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window


def clean_raw_prices(df: DataFrame) -> DataFrame:
    cleaned = (
        df.select(
            F.to_date("trade_date").alias("trade_date"),
            F.upper(F.col("ticker")).alias("ticker"),
            F.col("open").cast("double").alias("open"),
            F.col("high").cast("double").alias("high"),
            F.col("low").cast("double").alias("low"),
            F.col("close").cast("double").alias("close"),
            F.col("adj_close").cast("double").alias("adj_close"),
            F.col("volume").cast("double").alias("volume"),
            F.col("dividends").cast("double").alias("dividends"),
            F.col("stock_splits").cast("double").alias("stock_splits"),
            F.to_timestamp("ingestion_timestamp").alias("ingestion_timestamp"),
            F.col("source_system"),
        )
        .dropDuplicates(["ticker", "trade_date"])
        .filter(F.col("trade_date").isNotNull())
        .filter(F.col("close").isNotNull())
    )
    return cleaned.orderBy("ticker", "trade_date")


def add_technical_features(
    df: DataFrame,
    ma_windows: list[int],
    lag_days: list[int],
    rsi_window: int,
) -> DataFrame:
    ordered = Window.partitionBy("ticker").orderBy("trade_date")
    rsi_spec = ordered.rowsBetween(-rsi_window + 1, 0)

    transformed = (
        df.withColumn("previous_close", F.lag("close", 1).over(ordered))
        .withColumn("price_change", F.col("close") - F.col("previous_close"))
        .withColumn(
            "daily_return",
            F.when(
                F.col("previous_close").isNull(),
                None,
            ).otherwise((F.col("close") - F.col("previous_close")) / F.col("previous_close")),
        )
        .withColumn("gain", F.when(F.col("price_change") > 0, F.col("price_change")).otherwise(F.lit(0.0)))
        .withColumn("loss", F.when(F.col("price_change") < 0, -F.col("price_change")).otherwise(F.lit(0.0)))
        .withColumn("avg_gain", F.avg("gain").over(rsi_spec))
        .withColumn("avg_loss", F.avg("loss").over(rsi_spec))
        .withColumn(
            f"rsi_{rsi_window}",
            F.when(F.col("avg_loss") == 0, F.lit(100.0)).otherwise(
                100 - (100 / (1 + (F.col("avg_gain") / F.col("avg_loss"))))
            ),
        )
        .withColumn("day_of_week", F.dayofweek("trade_date"))
        .withColumn("month_of_year", F.month("trade_date"))
        .withColumn("year", F.year("trade_date"))
    )

    for window_size in ma_windows:
        moving_window = ordered.rowsBetween(-window_size + 1, 0)
        transformed = transformed.withColumn(f"ma_{window_size}", F.avg("close").over(moving_window))

    for lag_day in lag_days:
        transformed = transformed.withColumn(f"lag_{lag_day}", F.lag("close", lag_day).over(ordered))

    transformed = transformed.withColumn("target_close", F.lead("close", 1).over(ordered))

    return (
        transformed.drop("previous_close", "price_change", "gain", "loss", "avg_gain", "avg_loss")
        .filter(F.col("target_close").isNotNull())
        .orderBy("ticker", "trade_date")
    )

