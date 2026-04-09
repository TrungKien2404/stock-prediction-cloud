from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query

from api.schemas import MarketDataResponse, PredictionResponse
from api.service import get_prediction_service
from common.config import load_config


config = load_config()
app = FastAPI(
    title="Cloud Stock Analytics API",
    description="FastAPI service for serving stock predictions and processed market features.",
    version="1.0.0",
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/predict/{ticker}", response_model=PredictionResponse)
def predict(ticker: str) -> PredictionResponse:
    try:
        payload = get_prediction_service().get_latest_prediction(ticker)
    except KeyError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    return PredictionResponse(**payload)


@app.get("/data", response_model=list[MarketDataResponse])
def get_data(
    ticker: str | None = Query(default=None, description="Optional ticker filter, for example AAPL"),
    limit: int = Query(default=200, le=2000),
) -> list[MarketDataResponse]:
    return [MarketDataResponse(**row) for row in get_prediction_service().get_market_data(ticker=ticker, limit=limit)]

