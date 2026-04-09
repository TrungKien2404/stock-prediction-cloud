from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression, RandomForestRegressor
from pyspark.sql import DataFrame, Window, functions as F

from common.config import AppConfig, load_config
from common.io_utils import ensure_parent, write_json
from common.logging_utils import configure_logging
from common.path_utils import to_filesystem_path, to_spark_path
from common.spark_utils import get_spark_session, write_delta_table


LOGGER = logging.getLogger(__name__)


def build_feature_columns(config: AppConfig) -> list[str]:
    feature_columns = [
        "open",
        "high",
        "low",
        "close",
        "adj_close",
        "volume",
        "dividends",
        "stock_splits",
        "daily_return",
        "day_of_week",
        "month_of_year",
        "year",
    ]
    feature_columns.extend(f"ma_{window}" for window in config.features["ma_windows"])
    feature_columns.extend(f"lag_{lag_day}" for lag_day in config.features["lag_days"])
    feature_columns.append(f"rsi_{config.features['rsi_window']}")
    return feature_columns


def prepare_training_dataset(df: DataFrame, feature_columns: list[str], label_col: str) -> DataFrame:
    required_columns = feature_columns + [label_col, "ticker", "trade_date"]
    prepared = df.select(*required_columns).dropna(subset=required_columns)

    sequence_window = Window.partitionBy("ticker").orderBy("trade_date")
    count_window = Window.partitionBy("ticker")

    return (
        prepared.withColumn("row_num", F.row_number().over(sequence_window))
        .withColumn("row_count", F.count(F.lit(1)).over(count_window))
    )


def add_split_flag(df: DataFrame, train_ratio: float) -> DataFrame:
    return df.withColumn("split_boundary", F.floor(F.col("row_count") * F.lit(train_ratio))).withColumn(
        "dataset_split",
        F.when(F.col("row_num") <= F.col("split_boundary"), F.lit("train")).otherwise(F.lit("test")),
    )


def build_estimators(config: AppConfig) -> dict[str, object]:
    random_seed = int(config.model["random_seed"])
    return {
        "linear_regression": LinearRegression(
            featuresCol="features",
            labelCol=config.model["label_col"],
            predictionCol="prediction",
        ),
        "random_forest": RandomForestRegressor(
            featuresCol="features",
            labelCol=config.model["label_col"],
            predictionCol="prediction",
            seed=random_seed,
            numTrees=int(config.model["random_forest"]["num_trees"]),
            maxDepth=int(config.model["random_forest"]["max_depth"]),
        ),
        "gbt_regressor": GBTRegressor(
            featuresCol="features",
            labelCol=config.model["label_col"],
            predictionCol="prediction",
            seed=random_seed,
            maxIter=int(config.model["gbt"]["max_iter"]),
            maxDepth=int(config.model["gbt"]["max_depth"]),
        ),
    }


def evaluate_models(config: AppConfig, dataset: DataFrame) -> tuple[list[dict[str, float | str]], object, DataFrame]:
    feature_columns = build_feature_columns(config)
    training_frame = add_split_flag(
        prepare_training_dataset(dataset, feature_columns, config.model["label_col"]),
        train_ratio=float(config.model["train_ratio"]),
    )

    train_df = training_frame.filter(F.col("dataset_split") == "train")
    test_df = training_frame.filter(F.col("dataset_split") == "test")

    indexer = StringIndexer(inputCol="ticker", outputCol="ticker_index", handleInvalid="keep")
    assembler = VectorAssembler(
        inputCols=feature_columns + ["ticker_index"],
        outputCol="features",
        handleInvalid="skip",
    )
    evaluator = RegressionEvaluator(
        labelCol=config.model["label_col"],
        predictionCol="prediction",
        metricName="rmse",
    )

    results: list[dict[str, float | str]] = []
    best_rmse = float("inf")
    best_model = None
    best_predictions = None

    for model_name, estimator in build_estimators(config).items():
        pipeline = Pipeline(stages=[indexer, assembler, estimator])
        fitted_model = pipeline.fit(train_df)
        predictions = fitted_model.transform(test_df)
        rmse = float(evaluator.evaluate(predictions))

        metrics = {
            "model_name": model_name,
            "rmse": rmse,
        }
        results.append(metrics)
        LOGGER.info("Model=%s RMSE=%.4f", model_name, rmse)

        if rmse < best_rmse:
            best_rmse = rmse
            best_model = fitted_model
            best_predictions = predictions

    if best_model is None or best_predictions is None:
        raise RuntimeError("Model evaluation failed because no estimator produced predictions.")

    return results, best_model, best_predictions


def persist_artifacts(
    config: AppConfig,
    metrics: list[dict[str, float | str]],
    best_model_name: str,
    best_model: object,
    predictions: DataFrame,
) -> dict[str, str]:
    spark = predictions.sparkSession

    model_path = to_spark_path(config.resolve_path("model_path"))
    best_model.write().overwrite().save(model_path)

    metrics_json_path = to_filesystem_path(config.resolve_path("metrics_json_path"))
    write_json(metrics_json_path, {"metrics": metrics, "best_model": best_model_name})

    metrics_pd = pd.DataFrame(metrics).sort_values("rmse")
    metrics_parquet_path = Path(to_filesystem_path(config.resolve_path("metrics_parquet_path")))
    ensure_parent(metrics_parquet_path)
    metrics_pd.to_parquet(metrics_parquet_path, index=False)

    selected_columns = [
        "ticker",
        "trade_date",
        "prediction",
        config.model["label_col"],
    ]
    predictions_export = (
        predictions.select(*selected_columns)
        .withColumnRenamed(config.model["label_col"], "actual_close")
        .withColumn("model_name", F.lit(best_model_name))
        .withColumn("predicted_at", F.current_timestamp())
        .orderBy("ticker", "trade_date")
    )

    prediction_table_name = f"{config.storage['delta_database']}.{config.storage['delta_prediction_table']}"
    write_delta_table(
        df=predictions_export,
        table_name=prediction_table_name,
        storage_path=to_spark_path(config.resolve_path("delta_prediction_path")),
        mode="overwrite",
    )

    predictions_export_path = Path(to_filesystem_path(config.resolve_path("predictions_export_path")))
    ensure_parent(predictions_export_path)
    predictions_export.toPandas().to_parquet(predictions_export_path, index=False)

    return {
        "model_path": model_path,
        "metrics_json_path": str(metrics_json_path),
        "metrics_parquet_path": str(metrics_parquet_path),
        "prediction_table_name": prediction_table_name,
        "predictions_export_path": str(predictions_export_path),
    }


def run_training_pipeline(config_path: str = "configs/settings.yaml", base_path: str | None = None) -> dict[str, object]:
    config = load_config(config_path, base_path)
    spark = get_spark_session("stock_model_training")
    feature_table_name = f"{config.storage['delta_database']}.{config.storage['delta_feature_table']}"

    dataset = spark.table(feature_table_name)
    metrics, best_model, predictions = evaluate_models(config=config, dataset=dataset)
    best_model_name = min(metrics, key=lambda item: item["rmse"])["model_name"]
    artifact_paths = persist_artifacts(
        config=config,
        metrics=metrics,
        best_model_name=best_model_name,
        best_model=best_model,
        predictions=predictions,
    )
    return {
        "best_model": best_model_name,
        "metrics": metrics,
        **artifact_paths,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Spark ML training pipeline.")
    parser.add_argument("--config", default="configs/settings.yaml")
    parser.add_argument("--base-path", default=None)
    return parser.parse_args()


def main() -> None:
    configure_logging()
    args = parse_args()
    summary = run_training_pipeline(config_path=args.config, base_path=args.base_path)
    LOGGER.info("Training summary: %s", summary)


if __name__ == "__main__":
    main()
