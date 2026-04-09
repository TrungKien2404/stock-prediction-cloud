# Databricks notebook source
# COMMAND ----------
# MAGIC %pip install -r /Workspace/Repos/<your-user>/cloud2/requirements.txt

# COMMAND ----------
dbutils.widgets.text("project_root", "/Workspace/Repos/<your-user>/cloud2")
dbutils.widgets.text("config_path", "/Workspace/Repos/<your-user>/cloud2/configs/settings.yaml")
dbutils.widgets.text("base_path", "/dbfs/FileStore/cloud_stock_analytics")

# COMMAND ----------
import sys

project_root = dbutils.widgets.get("project_root")
config_path = dbutils.widgets.get("config_path")
base_path = dbutils.widgets.get("base_path")

if project_root not in sys.path:
    sys.path.append(project_root)

from dashboard.report import export_dashboard_html
from etl.pipeline import run_etl_pipeline
from ingestion.pipeline import run_ingestion_pipeline
from model.train import run_training_pipeline

# COMMAND ----------
ingestion_summary = run_ingestion_pipeline(config_path=config_path, base_path=base_path)
display(ingestion_summary)

# COMMAND ----------
etl_summary = run_etl_pipeline(config_path=config_path, base_path=base_path)
display(etl_summary)

# COMMAND ----------
training_summary = run_training_pipeline(config_path=config_path, base_path=base_path)
display(training_summary)

# COMMAND ----------
dashboard_html_path = export_dashboard_html(config_path=config_path, base_path=base_path)
display({"dashboard_html_path": dashboard_html_path})

