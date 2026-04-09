from __future__ import annotations

from dashboard.report import export_dashboard_html
from etl.pipeline import run_etl_pipeline
from ingestion.pipeline import run_ingestion_pipeline
from model.train import run_training_pipeline


def main() -> None:
    base_path = "."
    print("Step 1/4 - Ingestion")
    print(run_ingestion_pipeline(base_path=base_path))
    print("Step 2/4 - ETL")
    print(run_etl_pipeline(base_path=base_path))
    print("Step 3/4 - Training")
    print(run_training_pipeline(base_path=base_path))
    print("Step 4/4 - Dashboard export")
    print({"dashboard_html_path": export_dashboard_html(base_path=base_path)})


if __name__ == "__main__":
    main()

