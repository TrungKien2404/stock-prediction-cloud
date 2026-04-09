from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class PredictionResponse(BaseModel):
    ticker: str
    trade_date: str
    model_name: str
    predicted_close: float
    actual_close: Optional[float]
    predicted_at: datetime | None = None


class MarketDataResponse(BaseModel):
    ticker: str
    trade_date: str
    close: float
    ma_10: float | None = None
    ma_20: float | None = None
    rsi_14: float | None = None
