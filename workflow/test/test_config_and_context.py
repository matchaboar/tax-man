import os
from pathlib import Path

import pytest

from workflow import config as wf_config
from workflow.context import WorkflowContext


def test_discover_config_path_env_override(tmp_path: Path, monkeypatch):
    fake = tmp_path / "custom.yaml"
    monkeypatch.setenv(wf_config.ENV_CONFIG_PATH, str(fake))

    assert wf_config._discover_config_path() == fake


def test_discover_config_path_not_found(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        wf_config._discover_config_path(start=tmp_path)


def test_load_workflow_configs_validates_structure(tmp_path: Path):
    cfg = tmp_path / "workflows.yaml"
    cfg.write_text(
        """
workflows:
  - invalid
"""
    )

    with pytest.raises(wf_config.WorkflowConfigError):
        wf_config.load_workflow_configs(config_path=cfg)


def test_get_workflow_config_unknown_name(tmp_path: Path):
    cfg = tmp_path / "workflows.yaml"
    cfg.write_text(
        """
workflows:
  default:
    workflow: regex
selected: default
        """
    )

    with pytest.raises(wf_config.WorkflowConfigError):
        wf_config.get_workflow_config(config_path=cfg, config_name="missing")


def test_resolve_run_options_without_config(tmp_path: Path):
    resolved, applied = wf_config.resolve_run_options(
        config_name=None, overrides={}, config_path=tmp_path / "missing.yaml"
    )
    assert resolved["workflow"] == wf_config.DEFAULT_WORKFLOW_OPTIONS["workflow"]
    assert applied is None


def test_resolve_run_options_bubbles_config_error(tmp_path: Path):
    bad = tmp_path / "bad.yaml"
    bad.write_text(
        """
workflows:
  bad: []
"""
    )
    with pytest.raises(wf_config.WorkflowConfigError):
        wf_config.resolve_run_options(config_name="bad", overrides={}, config_path=bad)


def test_workflow_context_apply_updates_handles_unknown_key(tmp_path: Path):
    ctx = WorkflowContext(pdf_path=tmp_path / "doc.pdf")
    ctx.apply_updates({"metadata": {"a": 1}, "new_key": "value"})

    assert ctx.metadata["new_key"] == "value"


def test_workflow_context_add_error():
    ctx = WorkflowContext(pdf_path=Path("doc.pdf"))
    ctx.add_error("boom")

    assert ctx.errors == ["boom"]
