from pathlib import Path

import pytest

from strategy.extraction import ExtractNumericValues, ExtractRegexK1, InferExtractionCompleteness
from strategy.llm import MockOpenRouterExtractK1
from strategy.k1 import load_document_values
from strategy.models.k1.pydantic_model import GenericK1Lines
from workflow.context import WorkflowContext


FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "MockParsePdfToMarkdown"


@pytest.fixture
def pdf_path() -> Path:
    return FIXTURE_ROOT / "input_pdf_docs" / "doc_1.pdf"


@pytest.fixture
def parsed_markdown() -> str:
    fixture_path = FIXTURE_ROOT / "mock_markdown_response_body" / "doc_1.md"
    return fixture_path.read_text(encoding="utf-8")


def test_extract_numeric_values_updates_context_and_artifacts(pdf_path: Path, parsed_markdown: str):
    context = WorkflowContext(pdf_path=pdf_path, parsed_markdown=parsed_markdown)

    result = ExtractNumericValues().execute(context)
    result.merge_updates(context)

    assert result.output.numeric_fields["1"] == "34908"
    assert result.output.numeric_fields["4A"] == "3423"
    assert context.numeric_values["1"] == "34908"
    assert "4A" in result.artifacts["table_contexts"]


def test_extract_regex_k1_populates_field_values_and_metadata(pdf_path: Path, parsed_markdown: str):
    context = WorkflowContext(pdf_path=pdf_path, parsed_markdown=parsed_markdown)
    expected = load_document_values("doc_1.pdf")

    result = ExtractRegexK1().execute(context)
    result.merge_updates(context)

    assert context.field_values["partnership_name"] == expected["partnership_name"]
    assert context.field_values["line_4a_guaranteed_payments_for_services"] == expected["line_4a_guaranteed_payments_for_services"]
    assert isinstance(context.metadata["generic_lines"], GenericK1Lines)
    assert result.artifacts["used_strategies"]  # ensure strategies were tracked


def test_infer_extraction_completeness_flags_missing_required_fields(pdf_path: Path):
    context = WorkflowContext(
        pdf_path=pdf_path,
        field_values={
            "partnership_name": "",
            "partnership_employer_identification_number": "23-333333",
        },
    )

    result = InferExtractionCompleteness().execute(context)
    result.merge_updates(context)

    assert result.output["missing_required_fields"] == ["partnership_name"]
    assert context.inference["missing_required_fields"] == ["partnership_name"]


def test_mock_openrouter_extract_updates_field_values_and_metadata(pdf_path: Path, parsed_markdown: str):
    context = WorkflowContext(pdf_path=pdf_path, parsed_markdown=parsed_markdown)
    expected = load_document_values("doc_1.pdf")

    result = MockOpenRouterExtractK1().execute(context)
    result.merge_updates(context)

    assert context.field_values["partnership_name"] == expected["partnership_name"]
    assert context.field_values["line_4a_guaranteed_payments_for_services"] == expected["line_4a_guaranteed_payments_for_services"]
    assert isinstance(context.metadata["generic_lines"], GenericK1Lines)
    assert result.artifacts["source"] == "mock_openrouter"
