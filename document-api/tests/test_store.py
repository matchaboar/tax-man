import base64

from document_api.models import DocumentRecord, WorkflowRunResult
from document_api.store import InMemoryDocumentStore


def _record(doc_id: str = "abc") -> DocumentRecord:
    return DocumentRecord(
        id=doc_id,
        field_values={"a": "1"},
        numeric_values={},
        inference={},
        metadata={},
        artifacts={},
        trace=[],
        errors=[],
        succeeded=True,
    )


def test_save_and_list_round_trip():
    store = InMemoryDocumentStore()
    record = _record("doc-1")

    stored = store.save(record)

    assert stored is record
    assert store.get("doc-1") == record
    assert store.list_ids() == ["doc-1"]


def test_save_debug_persists_pdf_and_trace():
    store = InMemoryDocumentStore()
    record = _record("doc-2")
    trace = [
        {
            "name": "step",
            "strategy_name": "s",
            "strategy_version": "v",
            "input_context": {},
            "output": {"x": 1},
            "artifacts": {"a": 1},
            "errors": [],
            "post_context": {},
        }
    ]
    pdf_bytes = b"%PDF-1.4"

    debug_record = store.save_debug(
        document_record=record,
        pdf_bytes=pdf_bytes,
        pdf_filename="sample.pdf",
        trace=trace,
    )

    assert store.get("doc-2") == record
    assert store.get_debug("doc-2") == debug_record
    assert debug_record.pdf_filename == "sample.pdf"
    assert base64.b64decode(debug_record.pdf_base64.encode("ascii")) == pdf_bytes
    assert debug_record.steps[0].output == {"x": 1}
