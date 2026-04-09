# Databricks notebook source
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

from model.train import run_training_pipeline

summary = run_training_pipeline(config_path=config_path, base_path=base_path)
display(summary)

