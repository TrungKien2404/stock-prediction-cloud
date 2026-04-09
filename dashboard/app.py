from __future__ import annotations

from dash import Dash, Input, Output, dcc, html

from common.config import load_config
from dashboard.data_loader import load_dashboard_frames
from dashboard.report import build_actual_vs_prediction_figure, build_multistock_figure


config = load_config()
features_df, predictions_df = load_dashboard_frames()
available_tickers = sorted(predictions_df["ticker"].unique().tolist())

app = Dash(__name__)
app.title = "Cloud Stock Analytics Dashboard"

app.layout = html.Div(
    style={"fontFamily": "Segoe UI, sans-serif", "padding": "24px", "backgroundColor": "#f7f9fc"},
    children=[
        html.H1("Cloud Stock Analytics Dashboard"),
        html.P("Interactive Plotly dashboard for comparing actual and predicted stock prices."),
        dcc.Dropdown(
            id="ticker-dropdown",
            options=[{"label": ticker, "value": ticker} for ticker in available_tickers],
            value=available_tickers[0],
            clearable=False,
            style={"width": "320px"},
        ),
        dcc.Graph(id="actual-vs-prediction"),
        dcc.Graph(
            id="multi-stock-overview",
            figure=build_multistock_figure(features_df, lookback_days=int(config.dashboard["lookback_days"])),
        ),
    ],
)


@app.callback(Output("actual-vs-prediction", "figure"), Input("ticker-dropdown", "value"))
def update_prediction_chart(ticker: str):
    return build_actual_vs_prediction_figure(predictions_df, ticker)


server = app.server


if __name__ == "__main__":
    app.run(debug=True, host=config.dashboard["host"], port=int(config.dashboard["port"]))
