import base64
import json

import pytest
from fastapi.testclient import TestClient

from document_api.app import app, document_store, get_workflow_runner
from document_api.models import WorkflowRunResult


@pytest.fixture(autouse=True)
def reset_store():
    document_store.clear()
    yield
    document_store.clear()


@pytest.fixture
def client():
    return TestClient(app)


def _pdf_upload(filename: str = "doc_1.pdf"):
    return {"file": (filename, b"%PDF-1.4 test", "application/pdf")}


def test_upload_and_fetch_document(client: TestClient):
    response = client.post("/documents", files=_pdf_upload())
    assert response.status_code == 201
    payload = response.json()

    assert payload["succeeded"] is True
    assert payload["errors"] == []
    assert payload["field_values"]["partnership_name"] == "abc partnership"
    assert payload["field_values"]["partnership_employer_identification_number"] == "23-333333"

    document_id = payload["id"]

    list_response = client.get("/documents")
    assert list_response.status_code == 200
    assert document_id in list_response.json()["document_ids"]

    detail_response = client.get(f"/documents/{document_id}")
    assert detail_response.status_code == 200
    assert detail_response.json()["field_values"]["partnership_name"] == "abc partnership"


def test_workflow_runner_dependency_used(client: TestClient):
    calls = {}

    def stub_runner(
        *,
        pdf_path,
        workflow="regex",
        use_mock_parser=True,
        use_mock_llm=True,
        llm_model="openai/gpt-4o-mini",
        required_fields=None,
        strategy_version="v1.0.0",
        enable_wandb=False,
        wandb_project=None,
        wandb_entity=None,
        wandb_run_name=None,
        write_log_file=False,
        log_filename=None,
    ):
        calls.update(
            {
                "pdf_path": pdf_path,
                "workflow": workflow,
                "use_mock_parser": use_mock_parser,
                "use_mock_llm": use_mock_llm,
                "llm_model": llm_model,
                "required_fields": required_fields,
                "strategy_version": strategy_version,
                "enable_wandb": enable_wandb,
                "wandb_project": wandb_project,
                "wandb_entity": wandb_entity,
                "wandb_run_name": wandb_run_name,
                "write_log_file": write_log_file,
                "log_filename": log_filename,
            }
        )
        return WorkflowRunResult(
            succeeded=True,
            errors=[],
            field_values={"alpha": "beta"},
            numeric_values={"one": "1"},
            inference={"flags": []},
            metadata={"source": "stub"},
            artifacts={"parse": {"artifacts": {"source": "stub"}, "errors": []}},
        )

    app.dependency_overrides[get_workflow_runner] = lambda: stub_runner
    try:
        response = client.post(
            "/documents?workflow=llm&use_mock_parser=false&use_mock_llm=false&strategy_version=v2.0.0"
            "&llm_model=openai/gpt-4o&enable_wandb=true&wandb_project=test-proj&write_log_file=true"
            "&log_filename=custom.json",
            files=_pdf_upload("custom.pdf"),
        )
        assert response.status_code == 201
        assert calls["pdf_path"].name == "custom.pdf"
        assert calls["workflow"] == "llm"
        assert calls["use_mock_parser"] is False
        assert calls["use_mock_llm"] is False
        assert calls["strategy_version"] == "v2.0.0"
        assert calls["llm_model"] == "openai/gpt-4o"
        assert calls["enable_wandb"] is True
        assert calls["write_log_file"] is True
        assert calls["log_filename"] == "custom.json"

        payload = response.json()
        assert payload["field_values"]["alpha"] == "beta"
        assert payload["numeric_values"]["one"] == "1"
    finally:
        app.dependency_overrides.pop(get_workflow_runner, None)


def test_non_mock_parse_reports_missing_configuration(client: TestClient):
    response = client.post("/documents?use_mock_parser=false", files=_pdf_upload("doc_2.pdf"))
    assert response.status_code == 201
    payload = response.json()

    assert payload["succeeded"] is False
    assert any("DATALAB_API_KEY" in error for error in payload["errors"])
    assert payload["id"]


def test_write_log_file_creates_log(client: TestClient):
    from document_api.telemetry import DEFAULT_LOG_DIR

    filename = "api-run-test.json"
    target_log = DEFAULT_LOG_DIR / filename
    if target_log.exists():
        target_log.unlink()

    response = client.post(
        "/documents?write_log_file=true&log_filename=api-run-test.json",
        files=_pdf_upload(),
    )
    assert response.status_code == 201
    payload = response.json()
    assert payload["succeeded"] is True

    assert target_log.exists()
    data = json.loads(target_log.read_text())
    assert data["config"]["workflow"] == "regex"
    target_log.unlink()


def test_workflow_trace_endpoint(client: TestClient):
    upload = _pdf_upload("trace.pdf")
    pdf_bytes = upload["file"][1]
    response = client.post("/documents", files=upload)
    assert response.status_code == 201
    doc_id = response.json()["id"]

    trace_response = client.get(f"/workflow/{doc_id}")
    assert trace_response.status_code == 200
    body = trace_response.json()
    assert body["pdf_filename"] == "trace.pdf"
    decoded = base64.b64decode(body["pdf_base64"].encode("ascii"))
    assert decoded == pdf_bytes
    assert body["response_body"]["id"] == doc_id
    assert len(body["steps"]) >= 1
