import json
from pathlib import Path

import pytest

from strategy.llm import OpenRouterExtractK1, MockOpenRouterExtractK1
from workflow.context import WorkflowContext
from strategy.k1 import load_document_values


@pytest.fixture
def context_with_markdown(tmp_path: Path):
    pdf = tmp_path / "doc.pdf"
    pdf.write_text("pdf")
    ctx = WorkflowContext(pdf_path=pdf, parsed_markdown="sample markdown")
    return ctx


def test_build_messages_includes_fields():
    extractor = OpenRouterExtractK1(api_key="key")
    messages = extractor._build_messages("hello")

    assert messages[0]["role"] == "system"
    assert "Fields to extract" in messages[1]["content"]


def test_parse_response_populates_defaults():
    extractor = OpenRouterExtractK1(api_key="key", field_defaults={"x": "0"})
    raw = {"choices": [{"message": {"content": json.dumps({"x": "1"})}}]}

    parsed = extractor._parse_response(raw)

    assert parsed["x"] == "1"


def test_execute_uses_custom_request_func_and_updates_context(context_with_markdown):
    captured = {}

    def fake_request(api_key, base_url, payload):
        captured.update({"api_key": api_key, "base_url": base_url, "payload": payload})
        return {"choices": [{"message": {"content": json.dumps({"partnership_name": "abc"})}}]}

    extractor = OpenRouterExtractK1(api_key="key", request_func=fake_request)

    result = extractor.execute(context_with_markdown)
    result.merge_updates(context_with_markdown)

    assert result.output["partnership_name"] == "abc"
    assert captured["base_url"].startswith("https://")
    assert context_with_markdown.field_values["partnership_name"] == "abc"
    assert "generic_lines" in result.artifacts


def test_execute_missing_api_key_raises(context_with_markdown, monkeypatch):
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    extractor = OpenRouterExtractK1(api_key=None, request_func=lambda *_, **__: {})

    with pytest.raises(Exception):
        extractor.execute(context_with_markdown)


def test_mock_openrouter_respects_custom_values(context_with_markdown):
    custom_values = {"partnership_name": "custom", "line_4a_guaranteed_payments_for_services": "1"}
    extractor = MockOpenRouterExtractK1(mock_values=custom_values)

    result = extractor.execute(context_with_markdown)
    result.merge_updates(context_with_markdown)

    assert context_with_markdown.field_values["partnership_name"] == "custom"
    assert result.artifacts["source"] == "mock_openrouter"


def test_mock_openrouter_defaults_to_fixture(tmp_path: Path, parsed_markdown_fixture=None):
    # Uses real fixture values
    fixtures = Path(__file__).resolve().parent / "fixtures" / "MockParsePdfToMarkdown"
    pdf = fixtures / "input_pdf_docs" / "doc_1.pdf"
    ctx = WorkflowContext(pdf_path=pdf, parsed_markdown=(fixtures / "mock_markdown_response_body" / "doc_1.md").read_text())

    extractor = MockOpenRouterExtractK1()
    result = extractor.execute(ctx)

    expected = load_document_values("doc_1.pdf")
    assert result.output["partnership_name"] == expected["partnership_name"]
