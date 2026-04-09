from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / "configs" / "settings.yaml"


@dataclass(slots=True)
class AppConfig:
    """Thin wrapper around the YAML configuration file."""

    values: dict[str, Any]

    @property
    def project(self) -> dict[str, Any]:
        return self.values["project"]

    @property
    def stocks(self) -> dict[str, Any]:
        return self.values["stocks"]

    @property
    def storage(self) -> dict[str, Any]:
        return self.values["storage"]

    @property
    def features(self) -> dict[str, Any]:
        return self.values["features"]

    @property
    def model(self) -> dict[str, Any]:
        return self.values["model"]

    @property
    def dashboard(self) -> dict[str, Any]:
        return self.values["dashboard"]

    @property
    def api(self) -> dict[str, Any]:
        return self.values["api"]

    def with_base_path(self, base_path: str | Path | None = None) -> "AppConfig":
        if not base_path:
            return self

        cloned = yaml.safe_load(yaml.safe_dump(self.values))
        cloned["storage"]["base_path"] = str(base_path)
        return AppConfig(values=cloned)

    def resolve_path(self, key: str) -> Path:
        base_path = Path(self.storage["base_path"]).expanduser().resolve()
        target = Path(self.storage[key])
        if target.is_absolute():
            return target
        return (base_path / target).resolve()

    @property
    def tickers(self) -> list[str]:
        return [ticker.upper() for ticker in self.stocks["tickers"]]


def load_config(config_path: str | Path = DEFAULT_CONFIG_PATH, base_path: str | Path | None = None) -> AppConfig:
    config_file = Path(config_path).expanduser().resolve()
    with config_file.open("r", encoding="utf-8") as stream:
        values = yaml.safe_load(stream)
    return AppConfig(values=values).with_base_path(base_path)
