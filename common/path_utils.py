from __future__ import annotations

from pathlib import Path


def to_spark_path(path: str | Path) -> str:
    """Convert a local DBFS mount path into a Spark-compatible URI when needed."""
    value = str(path)
    if value.startswith("/dbfs/"):
        return value.replace("/dbfs/", "dbfs:/", 1)
    return value


def to_filesystem_path(path: str | Path) -> str:
    """Convert DBFS URI into driver filesystem path for pandas or Plotly exports."""
    value = str(path)
    if value.startswith("dbfs:/"):
        return value.replace("dbfs:/", "/dbfs/", 1)
    return str(Path(value))

