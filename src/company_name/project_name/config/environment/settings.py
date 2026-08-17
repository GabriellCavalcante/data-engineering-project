from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

from company_name.project_name.common.constants.application_constants import (
    CONFIG_FILE_ENV_VAR,
    DEFAULT_CONFIG_FILE,
)
from company_name.project_name.common.exceptions.processing_exception import ProcessingException


class Settings:
    """Carrega configurações externas sem espalhar leitura de YAML pelo projeto."""

    def __init__(self, values: dict[str, Any]):
        self._values = values

    @classmethod
    def load(cls, path: str | Path | None = None) -> "Settings":
        config_path = Path(
            path or os.getenv(CONFIG_FILE_ENV_VAR, DEFAULT_CONFIG_FILE)
        )
        if not config_path.exists():
            raise ProcessingException(
                f"Arquivo de configuração não encontrado: {config_path}"
            )

        with config_path.open("r", encoding="utf-8") as file:
            values = yaml.safe_load(file) or {}

        return cls(values)

    def get(self, key: str, default: Any = None) -> Any:
        current: Any = self._values
        for part in key.split("."):
            if not isinstance(current, dict) or part not in current:
                return default
            current = current[part]
        return current

    def require(self, key: str) -> Any:
        value = self.get(key)
        if value is None or value == "":
            raise ProcessingException(f"Configuração obrigatória ausente: {key}")
        return value
