from pathlib import Path

import pytest

from document_api import workflow_runner
from document_api.workflow_runner import _resolve_run_config, _build_k1, run_k1_workflow
from document_api.models import WorkflowRunResult


FIXTURE_PDF = (
    Path(__file__).resolve().parents[2]
    / "strategy"
    / "test"
    / "fixtures"
    / "MockParsePdfToMarkdown"
    / "input_pdf_docs"
    / "doc_1.pdf"
)


def test_resolve_run_config_normalizes_required_fields():
    resolved, applied = _resolve_run_config(
        workflow_config=None,
        workflow_config_path=None,
        workflow="regex",
        use_mock_parser=True,
        use_mock_llm=True,
        llm_model="model",
        required_fields={"a", "b"},
        strategy_version="v1",
    )

    assert isinstance(resolved["required_fields"], list)
    assert resolved["required_fields"] == ["a", "b"] or sorted(resolved["required_fields"]) == ["a", "b"]
    assert applied in (None, "production")


def test_build_k1_invalid_workflow_raises():
    with pytest.raises(ValueError):
        _build_k1(
            workflow="unknown",
            pdf_path=FIXTURE_PDF,
            use_mock_parser=True,
            use_mock_llm=True,
            llm_model="model",
            required_fields=None,
            strategy_version="v1",
        )


def test_run_k1_workflow_returns_failure_on_missing_config(tmp_path: Path):
    missing_config = tmp_path / "does-not-exist.yaml"
    result = run_k1_workflow(
        pdf_path=FIXTURE_PDF,
        workflow_config="custom",
        workflow_config_path=missing_config,
    )

    assert isinstance(result, WorkflowRunResult)
    assert result.succeeded is False
    assert "does-not-exist" in result.errors[0]


def test_run_k1_workflow_records_wandb_url(monkeypatch):
    called = {}

    def fake_record_wandb_run(**kwargs):
        called.update(kwargs)
        return "http://wandb.url/run"

    monkeypatch.setattr(workflow_runner, "record_wandb_run", fake_record_wandb_run)

    result = run_k1_workflow(
        pdf_path=FIXTURE_PDF,
        enable_wandb=True,
        wandb_project="proj",
    )

    assert result.metadata["wandb_run_url"] == "http://wandb.url/run"
    assert called["project"] == "proj"
