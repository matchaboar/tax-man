from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple

import yaml

# Base defaults so callers can fall back when configs are missing.
DEFAULT_WORKFLOW_OPTIONS: Dict[str, Any] = {
    "workflow": "regex",
    "use_mock_parser": True,
    "use_mock_llm": True,
    "llm_model": "openai/gpt-4o-mini",
    "strategy_version": "v1.0.0",
    "required_fields": None,
}

DEFAULT_CONFIG_FILENAME = "workflows.yaml"
DEFAULT_CONFIG_DIRNAME = "config"
ENV_CONFIG_PATH = "WORKFLOW_CONFIG_PATH"


class WorkflowConfigError(ValueError):
    """Raised when a workflow configuration file is invalid or incomplete."""


@dataclass
class WorkflowConfig:
    """One workflow configuration preset loaded from YAML."""

    name: str
    workflow: str
    use_mock_parser: bool
    use_mock_llm: bool
    llm_model: str
    strategy_version: str
    required_fields: Optional[Iterable[str]] = None
    description: Optional[str] = None

    def to_kwargs(self) -> Dict[str, Any]:
        """Flatten the config so it can be passed into run_k1_workflow."""
        return {
            "workflow": self.workflow,
            "use_mock_parser": self.use_mock_parser,
            "use_mock_llm": self.use_mock_llm,
            "llm_model": self.llm_model,
            "strategy_version": self.strategy_version,
            "required_fields": list(self.required_fields) if self.required_fields else None,
        }


def _discover_config_path(start: Optional[Path] = None) -> Path:
    """Locate the config file starting from `start` or this file and walking up."""
    env_override = os.getenv(ENV_CONFIG_PATH)
    if env_override:
        return Path(env_override)

    search_root = start or Path(__file__).resolve()
    for candidate in [search_root, *search_root.parents]:
        config_path = candidate / DEFAULT_CONFIG_DIRNAME / DEFAULT_CONFIG_FILENAME
        if config_path.exists():
            return config_path
    raise FileNotFoundError("Workflow config file not found; expected config/workflows.yaml")


def load_workflow_configs(
    config_path: Optional[Path] = None, start: Optional[Path] = None
) -> Tuple[Optional[str], Dict[str, WorkflowConfig]]:
    """Load all workflow configs from YAML."""
    path = Path(config_path) if config_path else _discover_config_path(start=start)
    if not path.exists():
        raise FileNotFoundError(f"Workflow config file not found at {path}")

    raw = yaml.safe_load(path.read_text()) or {}
    selected = raw.get("selected")
    raw_workflows = raw.get("workflows") or {}
    if not isinstance(raw_workflows, Mapping):
        raise WorkflowConfigError("`workflows` must be a mapping of config name to options.")

    configs: Dict[str, WorkflowConfig] = {}
    for name, body in raw_workflows.items():
        if not isinstance(body, Mapping):
            raise WorkflowConfigError(f"Workflow config '{name}' must be a mapping.")

        merged: Dict[str, Any] = dict(DEFAULT_WORKFLOW_OPTIONS)
        merged.update({k: v for k, v in body.items() if v is not None})
        configs[name] = WorkflowConfig(
            name=name,
            description=body.get("description"),
            workflow=str(merged["workflow"]),
            use_mock_parser=bool(merged["use_mock_parser"]),
            use_mock_llm=bool(merged["use_mock_llm"]),
            llm_model=str(merged["llm_model"]),
            strategy_version=str(merged["strategy_version"]),
            required_fields=(
                list(merged["required_fields"])
                if merged.get("required_fields") is not None
                else None
            ),
        )
    return selected, configs


def get_workflow_config(
    *,
    config_name: Optional[str] = None,
    config_path: Optional[Path] = None,
    start: Optional[Path] = None,
) -> WorkflowConfig:
    """Return a single workflow config. Prefers the named config, else the selected one."""
    selected, configs = load_workflow_configs(config_path=config_path, start=start)
    if not configs:
        raise WorkflowConfigError("No workflow configs defined.")

    target = config_name or selected
    if not target:
        raise WorkflowConfigError("No workflow config selected; set 'selected' or pass a name.")

    if target not in configs:
        available = ", ".join(sorted(configs))
        raise WorkflowConfigError(f"Unknown workflow config '{target}'. Available: {available}")
    return configs[target]


def resolve_run_options(
    *,
    config_name: Optional[str],
    overrides: Mapping[str, Any],
    config_path: Optional[Path] = None,
    defaults: Optional[Mapping[str, Any]] = None,
) -> Tuple[Dict[str, Any], Optional[str]]:
    """
    Merge defaults, YAML config, and explicit overrides into a run config dict.

    Returns (resolved_options, applied_config_name).
    """
    base = dict(DEFAULT_WORKFLOW_OPTIONS)
    if defaults:
        base.update({k: v for k, v in defaults.items() if v is not None})

    config_entry: Optional[WorkflowConfig] = None
    try:
        config_entry = get_workflow_config(config_name=config_name, config_path=config_path)
    except FileNotFoundError:
        # No config file on disk: fall back to defaults unless a specific name was requested.
        if config_name:
            raise
    except WorkflowConfigError:
        # If the caller asked for a specific config, bubble up; otherwise, ignore and use defaults.
        if config_name:
            raise

    if config_entry:
        base.update(config_entry.to_kwargs())

    for key, value in overrides.items():
        if value is not None:
            base[key] = value

    applied_name = config_entry.name if config_entry else None
    return base, applied_name
