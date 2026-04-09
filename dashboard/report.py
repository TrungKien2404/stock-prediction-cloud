from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from common.config import load_config
from common.io_utils import ensure_parent
from common.path_utils import to_filesystem_path
from dashboard.data_loader import load_dashboard_frames


def build_actual_vs_prediction_figure(predictions: pd.DataFrame, ticker: str) -> go.Figure:
    filtered = predictions.loc[predictions["ticker"] == ticker].copy()
    figure = go.Figure()
    figure.add_trace(
        go.Scatter(
            x=filtered["trade_date"],
            y=filtered["actual_close"],
            mode="lines",
            name="Actual Close",
        )
    )
    figure.add_trace(
        go.Scatter(
            x=filtered["trade_date"],
            y=filtered["prediction"],
            mode="lines",
            name="Predicted Close",
        )
    )
    figure.update_layout(title=f"{ticker} Actual vs Predicted Close", xaxis_title="Date", yaxis_title="Price")
    return figure


def build_multistock_figure(features: pd.DataFrame, lookback_days: int) -> go.Figure:
    cutoff = features["trade_date"].max() - pd.Timedelta(days=lookback_days)
    filtered = features.loc[features["trade_date"] >= cutoff].copy()
    return px.line(
        filtered,
        x="trade_date",
        y="close",
        color="ticker",
        title=f"Multi-stock Close Price Comparison (last {lookback_days} days)",
    )


def export_dashboard_html(config_path: str = "configs/settings.yaml", base_path: str | None = None) -> str:
    config = load_config(config_path, base_path)
    features, predictions = load_dashboard_frames(config_path=config_path, base_path=base_path)

    tickers = sorted(predictions["ticker"].unique())
    latest_ticker = tickers[0]
    fig_price = build_actual_vs_prediction_figure(predictions, latest_ticker)
    fig_multi = build_multistock_figure(features, lookback_days=int(config.dashboard["lookback_days"]))

    composed = make_subplots(
        rows=2,
        cols=1,
        subplot_titles=(
            f"{latest_ticker} - Actual vs Predicted",
            "Multi-stock price visualization",
        ),
        vertical_spacing=0.16,
    )
    for trace in fig_price.data:
        composed.add_trace(trace, row=1, col=1)
    for trace in fig_multi.data:
        composed.add_trace(trace, row=2, col=1)

    composed.update_layout(
        height=950,
        title="Cloud Stock Analytics Dashboard",
        template="plotly_white",
    )

    output_path = Path(to_filesystem_path(config.resolve_path("dashboard_html_path")))
    ensure_parent(output_path)
    output_path.write_text(composed.to_html(full_html=True, include_plotlyjs="cdn"), encoding="utf-8")
    return str(output_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate Plotly HTML dashboard artifact.")
    parser.add_argument("--config", default="configs/settings.yaml")
    parser.add_argument("--base-path", default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    path = export_dashboard_html(config_path=args.config, base_path=args.base_path)
    print({"dashboard_html_path": path})


if __name__ == "__main__":
    main()

