import os
import sys
from pathlib import Path

import pytest

from document_api.telemetry import _coerce_filename, log_to_wandb, write_run_log
from document_api.models import WorkflowRunResult


def test_coerce_filename_strips_invalid_characters():
    assert _coerce_filename(None) == "last_run.json"
    assert _coerce_filename("!weird name?.json") == "weirdname.json"


def test_write_run_log_writes_json(tmp_path: Path):
    result = WorkflowRunResult(succeeded=True)
    path = write_run_log(result=result, config={"workflow": "regex"}, filename="run.json", log_dir=tmp_path)

    assert path.exists()
    content = path.read_text()
    assert "workflow" in content
    assert "timestamp" in content


def test_log_to_wandb_respects_env_and_returns_url(monkeypatch, tmp_path: Path):
    result = WorkflowRunResult(succeeded=True, field_values={"a": 1})

    class FakeRun:
        def __init__(self):
            self.summary = {}
            self.logged = []
            self.url = "http://wandb.test/run"

        def log(self, data):
            self.logged.append(data)

        def finish(self):
            self.finished = True

    class FakeWandb:
        def __init__(self):
            self.runs = []

        def init(self, **kwargs):
            self.runs.append(kwargs)
            return FakeRun()

    fake_wandb = FakeWandb()
    monkeypatch.setitem(sys.modules, "wandb", fake_wandb)
    monkeypatch.setenv("WANDB_API_KEY", "test-key")
    monkeypatch.delenv("WANDB_DISABLED", raising=False)

    url = log_to_wandb(result=result, config={"x": 1}, project="proj", entity="ent", run_name="run")

    assert url == "http://wandb.test/run"
    assert fake_wandb.runs[0]["project"] == "proj"


def test_log_to_wandb_disabled_returns_none(monkeypatch):
    result = WorkflowRunResult(succeeded=True)
    monkeypatch.setenv("WANDB_DISABLED", "true")
    monkeypatch.setenv("WANDB_API_KEY", "key")
    monkeypatch.setitem(sys.modules, "wandb", object())

    assert log_to_wandb(result=result, config={}) is None
