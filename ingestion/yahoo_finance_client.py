from __future__ import annotations

import logging
from dataclasses import dataclass

import pandas as pd
import yfinance as yf


LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class YahooFinanceClient:
    """Wrapper around yfinance to keep the ingestion service testable and focused."""

    @staticmethod
    def _flatten_columns(frame: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(frame.columns, pd.MultiIndex):
            frame.columns = [str(column).strip().lower().replace(" ", "_") for column in frame.columns]
            return frame

        flattened: list[str] = []
        for column in frame.columns:
            non_empty_parts = [str(part) for part in column if str(part) not in {"", "None"}]
            preferred = non_empty_parts[0] if non_empty_parts else "value"
            flattened.append(preferred.strip().lower().replace(" ", "_"))
        frame.columns = flattened
        return frame

    def fetch_history(
        self,
        ticker: str,
        start_date: str,
        end_date: str | None = None,
        interval: str = "1d",
    ) -> pd.DataFrame:
        LOGGER.info("Downloading %s data from Yahoo Finance", ticker)
        frame = yf.download(
            tickers=ticker,
            start=start_date,
            end=end_date,
            interval=interval,
            auto_adjust=False,
            progress=False,
            actions=True,
            group_by="column",
        )
        if frame.empty:
            raise ValueError(f"No data returned for ticker={ticker}")

        frame = frame.reset_index()
        frame = self._flatten_columns(frame)
        frame["ticker"] = ticker.upper()
        frame["ingestion_timestamp"] = pd.Timestamp.utcnow()
        frame["source_system"] = "yahoo_finance"
        return frame.rename(
            columns={
                "date": "trade_date",
                "adj_close": "adj_close",
                "stock_splits": "stock_splits",
            }
        )
