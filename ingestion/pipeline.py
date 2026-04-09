from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from common.config import AppConfig, load_config
from common.io_utils import ensure_directory, write_json
from common.logging_utils import configure_logging
from common.path_utils import to_filesystem_path
from ingestion.yahoo_finance_client import YahooFinanceClient


LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class IngestionSummary:
    tickers: list[str]
    record_count: int
    raw_output_path: str
    manifest_path: str


class YahooFinanceIngestionService:
    def __init__(self, config: AppConfig, client: YahooFinanceClient | None = None) -> None:
        self.config = config
        self.client = client or YahooFinanceClient()

    def run(self) -> IngestionSummary:
        raw_path = Path(to_filesystem_path(self.config.resolve_path("raw_parquet_path")))
        ensure_directory(raw_path)

        frames: list[pd.DataFrame] = []
        for ticker in self.config.tickers:
            history = self.client.fetch_history(
                ticker=ticker,
                start_date=self.config.stocks["start_date"],
                end_date=self.config.stocks["end_date"],
                interval=self.config.stocks["interval"],
            )
            frames.append(history)

            ticker_path = raw_path / f"ticker={ticker}"
            ensure_directory(ticker_path)
            history.to_parquet(ticker_path / "part-000.parquet", index=False)

        combined = pd.concat(frames, ignore_index=True).sort_values(["ticker", "trade_date"])
        manifest = {
            "project": self.config.project["name"],
            "tickers": self.config.tickers,
            "record_count": int(len(combined)),
            "date_range": {
                "min_trade_date": combined["trade_date"].min(),
                "max_trade_date": combined["trade_date"].max(),
            },
            "raw_output_path": str(raw_path),
        }
        manifest_path = self.config.resolve_path("ingestion_manifest_path")
        write_json(to_filesystem_path(manifest_path), manifest)

        LOGGER.info("Ingestion completed with %s rows", len(combined))
        return IngestionSummary(
            tickers=self.config.tickers,
            record_count=int(len(combined)),
            raw_output_path=str(raw_path),
            manifest_path=str(manifest_path),
        )


def run_ingestion_pipeline(config_path: str = "configs/settings.yaml", base_path: str | None = None) -> dict[str, str | int | list[str]]:
    config = load_config(config_path, base_path)
    summary = YahooFinanceIngestionService(config=config).run()
    return {
        "tickers": summary.tickers,
        "record_count": summary.record_count,
        "raw_output_path": summary.raw_output_path,
        "manifest_path": summary.manifest_path,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Yahoo Finance ingestion pipeline.")
    parser.add_argument("--config", default="configs/settings.yaml")
    parser.add_argument("--base-path", default=None)
    return parser.parse_args()


def main() -> None:
    configure_logging()
    args = parse_args()
    summary = run_ingestion_pipeline(config_path=args.config, base_path=args.base_path)
    LOGGER.info("Ingestion summary: %s", summary)


if __name__ == "__main__":
    main()
