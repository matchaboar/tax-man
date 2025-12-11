from pathlib import Path

import pytest

from strategy.extraction import ExtractRegexK1
from strategy.parse import MockParsePdfToDatalabMarkdown
from strategy.k1 import load_document_values
from workflow.context import WorkflowContext
from workflow.k1 import build_k1_workflow, build_k1_llm_extract_workflow


FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "MockParsePdfToMarkdown"


def test_mock_parser_loads_markdown():
    pdf_path = FIXTURE_ROOT / "input_pdf_docs" / "doc_1.pdf"
    context = WorkflowContext(pdf_path=pdf_path)
    strategy = MockParsePdfToDatalabMarkdown(fixture_root=FIXTURE_ROOT)

    result = strategy.execute(context)

    assert "Schedule K-1" in result.output
    assert result.context_updates["parsed_markdown"].startswith("Schedule K-1")


def test_regex_extractor_matches_known_fields():
    pdf_path = FIXTURE_ROOT / "input_pdf_docs" / "doc_1.pdf"
    markdown = (
        FIXTURE_ROOT / "mock_markdown_response_body" / "doc_1.md"
    ).read_text(encoding="utf-8")
    context = WorkflowContext(pdf_path=pdf_path, parsed_markdown=markdown)

    strategy = ExtractRegexK1()
    result = strategy.execute(context)
    expected = load_document_values("doc_1.pdf")

    for field in [
        "partnership_name",
        "partnership_employer_identification_number",
        "line_4a_guaranteed_payments_for_services",
        "line_9b_collectibles_28_percent_gain_loss",
    ]:
        assert result.output[field] == expected[field]


def test_k1_workflow_runs_end_to_end():
    pdf_path = FIXTURE_ROOT / "input_pdf_docs" / "doc_1.pdf"
    workflow, context = build_k1_workflow(pdf_path=pdf_path, use_mock_parser=True)

    result = workflow.run(context)

    assert result.succeeded
    assert context.parsed_markdown
    assert context.field_values.get("partnership_name") == "abc partnership"
    assert any(r.name == "infer" for r in result.activity_results)


def test_k1_llm_workflow_uses_mock_openrouter():
    pdf_path = FIXTURE_ROOT / "input_pdf_docs" / "doc_1.pdf"
    workflow, context = build_k1_llm_extract_workflow(
        pdf_path=pdf_path,
        use_mock_parser=True,
        use_mock_llm=True,
    )

    result = workflow.run(context)

    assert result.succeeded
    expected = load_document_values("doc_1.pdf")
    for field in [
        "partnership_name",
        "partnership_employer_identification_number",
        "line_4a_guaranteed_payments_for_services",
    ]:
        assert context.field_values[field] == expected[field]
