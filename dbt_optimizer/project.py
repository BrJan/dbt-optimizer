"""dbt project discovery and parsing."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

from .models import DbtModel


class DbtProjectError(Exception):
    pass


class DbtProjectParser:
    """Discovers and parses a dbt project on disk."""

    def __init__(self, project_path: str | Path) -> None:
        self.project_path = Path(project_path).resolve()
        self._project_config: dict[str, Any] = {}
        self._schema_configs: dict[str, dict] = {}  # model_name -> schema config

    def load(self) -> "DbtProjectParser":
        """Load project config and schema files."""
        self._project_config = self._load_project_config()
        self._schema_configs = self._load_schema_configs()
        return self

    @property
    def project_name(self) -> str:
        return self._project_config.get("name", self.project_path.name)

    @property
    def models_path(self) -> Path:
        paths = self._project_config.get("model-paths", self._project_config.get("source-paths", ["models"]))
        # Use first path
        return self.project_path / paths[0]

    def _load_project_config(self) -> dict[str, Any]:
        config_file = self.project_path / "dbt_project.yml"
        if not config_file.exists():
            raise DbtProjectError(
                f"No dbt_project.yml found at {self.project_path}. "
                "Make sure you're pointing at the root of a dbt project."
            )
        with open(config_file) as f:
            return yaml.safe_load(f) or {}

    def _load_schema_configs(self) -> dict[str, dict]:
        """Parse all schema.yml / properties YAML files for model metadata."""
        configs: dict[str, dict] = {}
        if not self.models_path.exists():
            return configs

        for yaml_file in self.models_path.rglob("*.yml"):
            try:
                with open(yaml_file) as f:
                    data = yaml.safe_load(f) or {}
                for model_def in data.get("models", []):
                    name = model_def.get("name", "")
                    if name:
                        configs[name] = model_def
            except Exception:
                pass  # Tolerate malformed YAML

        return configs

    def _get_materialization(self, model_name: str, sql: str) -> str:
        """Resolve materialization: config() block > schema config > project default > 'view'."""
        # 1. Check inline config() call
        match = re.search(
            r"\{\{\s*config\s*\([^)]*materialized\s*=\s*['\"](\w+)['\"]", sql, re.IGNORECASE
        )
        if match:
            return match.group(1)

        # 2. Check schema.yml config
        schema = self._schema_configs.get(model_name, {})
        mat = schema.get("config", {}).get("materialized")
        if mat:
            return mat

        # 3. Project-level model config (simplified: check top-level models key)
        models_config = self._project_config.get("models", {})
        project_mat = self._dig_materialization(models_config)
        if project_mat:
            return project_mat

        return "view"

    def _dig_materialization(self, config: Any) -> str | None:
        if isinstance(config, dict):
            if "+materialized" in config:
                return config["+materialized"]
            if "materialized" in config:
                return config["materialized"]
            for v in config.values():
                result = self._dig_materialization(v)
                if result:
                    return result
        return None

    def discover_models(self) -> list[DbtModel]:
        """Return all SQL model files in the project."""
        if not self.models_path.exists():
            raise DbtProjectError(f"Models directory not found: {self.models_path}")

        models: list[DbtModel] = []
        for sql_file in sorted(self.models_path.rglob("*.sql")):
            # Skip test files (schema tests use .sql too in some versions)
            if sql_file.parent.name in ("tests", "snapshots", "analyses"):
                continue

            try:
                sql = sql_file.read_text(encoding="utf-8")
            except Exception:
                continue

            name = sql_file.stem
            materialization = self._get_materialization(name, sql)
            schema = self._schema_configs.get(name, {})
            columns = [c.get("name", "") for c in schema.get("columns", [])]
            has_tests = bool(schema.get("tests") or any(c.get("tests") for c in schema.get("columns", [])))
            has_description = bool(schema.get("description", "").strip())

            models.append(
                DbtModel(
                    name=name,
                    path=sql_file.relative_to(self.project_path),
                    sql=sql,
                    materialization=materialization,
                    schema_config=schema,
                    columns=columns,
                    has_tests=has_tests,
                    has_description=has_description,
                )
            )

        return models
