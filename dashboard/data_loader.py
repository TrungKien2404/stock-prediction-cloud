from __future__ import annotations

import pandas as pd

from common.config import load_config
from common.path_utils import to_filesystem_path


def load_dashboard_frames(config_path: str = "configs/settings.yaml", base_path: str | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    config = load_config(config_path, base_path)
    features_path = to_filesystem_path(config.resolve_path("processed_parquet_path"))
    predictions_path = to_filesystem_path(config.resolve_path("predictions_export_path"))

    features = pd.read_parquet(features_path)
    predictions = pd.read_parquet(predictions_path)

    features["trade_date"] = pd.to_datetime(features["trade_date"])
    predictions["trade_date"] = pd.to_datetime(predictions["trade_date"])
    return features.sort_values(["ticker", "trade_date"]), predictions.sort_values(["ticker", "trade_date"])

