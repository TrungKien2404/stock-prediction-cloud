from __future__ import annotations

from functools import lru_cache

import pandas as pd

from common.config import load_config
from common.path_utils import to_filesystem_path


class PredictionService:
    def __init__(self, config_path: str = "configs/settings.yaml", base_path: str | None = None) -> None:
        self.config = load_config(config_path, base_path)
        self.predictions_path = to_filesystem_path(self.config.resolve_path("predictions_export_path"))
        self.features_path = to_filesystem_path(self.config.resolve_path("processed_parquet_path"))

    def load_predictions(self) -> pd.DataFrame:
        frame = pd.read_parquet(self.predictions_path)
        frame["trade_date"] = pd.to_datetime(frame["trade_date"])
        return frame.sort_values(["ticker", "trade_date"])

    def load_features(self) -> pd.DataFrame:
        frame = pd.read_parquet(self.features_path)
        frame["trade_date"] = pd.to_datetime(frame["trade_date"])
        return frame.sort_values(["ticker", "trade_date"])

    def get_latest_prediction(self, ticker: str) -> dict[str, object]:
        frame = self.load_predictions()
        filtered = frame.loc[frame["ticker"] == ticker.upper()]
        if filtered.empty:
            raise KeyError(f"Ticker {ticker} not found in predictions.")

        latest = filtered.iloc[-1]
        return {
            "ticker": latest["ticker"],
            "trade_date": latest["trade_date"].date().isoformat(),
            "model_name": str(latest["model_name"]),
            "predicted_close": float(latest["prediction"]),
            "actual_close": float(latest["actual_close"]) if pd.notna(latest["actual_close"]) else None,
            "predicted_at": latest.get("predicted_at"),
        }

    def get_market_data(self, ticker: str | None = None, limit: int = 200) -> list[dict[str, object]]:
        frame = self.load_features()
        if ticker:
            frame = frame.loc[frame["ticker"] == ticker.upper()]

        selected = frame.tail(limit)[["ticker", "trade_date", "close", "ma_10", "ma_20", "rsi_14"]]
        selected["trade_date"] = selected["trade_date"].dt.date.astype(str)
        return selected.to_dict(orient="records")


@lru_cache(maxsize=1)
def get_prediction_service(config_path: str = "configs/settings.yaml", base_path: str | None = None) -> PredictionService:
    return PredictionService(config_path=config_path, base_path=base_path)
