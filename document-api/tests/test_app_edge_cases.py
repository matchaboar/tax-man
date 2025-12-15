import base64
from fastapi.testclient import TestClient
import pytest

from document_api.app import app, document_store


@pytest.fixture(autouse=True)
def reset_store():
    document_store.clear()
    yield
    document_store.clear()


@pytest.fixture
def client():
    return TestClient(app)


def _upload(file_tuple):
    return {"file": file_tuple}


def test_upload_without_filename_returns_400(client: TestClient):
    from document_api.app import _validate_upload

    class DummyFile:
        def __init__(self):
            self.filename = ""
            self.content_type = "application/pdf"

    with pytest.raises(Exception):
        _validate_upload(DummyFile())


def test_upload_with_bad_content_type_returns_415(client: TestClient):
    response = client.post(
        "/documents", files=_upload(("doc.txt", b"hello", "text/plain"))
    )
    assert response.status_code == 415
    assert "Only PDF uploads" in response.json()["detail"]


def test_empty_pdf_payload_rejected(client: TestClient):
    response = client.post(
        "/documents", files=_upload(("empty.pdf", b"", "application/pdf"))
    )
    assert response.status_code == 400
    assert response.json()["detail"] == "Uploaded file is empty"


def test_get_document_not_found(client: TestClient):
    response = client.get("/documents/missing")
    assert response.status_code == 404


def test_get_workflow_not_found(client: TestClient):
    response = client.get("/workflow/missing")
    assert response.status_code == 404


def test_workflow_view_renders_html(client: TestClient):
    response = client.get("/workflow/view?document_id=abc")
    assert response.status_code == 200
    assert "Workflow Viewer" in response.text
    assert "abc" in response.text


def test_workflow_ui_alias_matches_view(client: TestClient):
    view = client.get("/workflow/view?document_id=xyz")
    ui = client.get("/workflow/ui?document_id=xyz")
    assert view.text == ui.text


def test_workflow_trace_base64_round_trip(client: TestClient):
    upload = _upload(("trace2.pdf", b"%PDF-1.4 data", "application/pdf"))
    create = client.post("/documents", files=upload)
    doc_id = create.json()["id"]

    trace_response = client.get(f"/workflow/{doc_id}")
    assert trace_response.status_code == 200
    payload = trace_response.json()

    decoded = base64.b64decode(payload["pdf_base64"].encode("ascii"))
    assert decoded == upload["file"][1]
    assert payload["pdf_filename"] == "trace2.pdf"
