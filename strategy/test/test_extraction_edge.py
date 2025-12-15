import pytest

from strategy.extraction import ExtractNumericValues, ExtractRegexK1, InferExtractionCompleteness
from workflow.context import WorkflowContext


def test_extract_numeric_values_requires_markdown(tmp_path):
    context = WorkflowContext(pdf_path=tmp_path / "missing.pdf")
    strategy = ExtractNumericValues()
    with pytest.raises(Exception):
        strategy.execute(context)


def test_extract_regex_requires_markdown(tmp_path):
    context = WorkflowContext(pdf_path=tmp_path / "missing.pdf")
    strategy = ExtractRegexK1()
    with pytest.raises(Exception):
        strategy.execute(context)


def test_infer_extraction_completeness_uses_required_fields(tmp_path):
    context = WorkflowContext(pdf_path=tmp_path / "x.pdf", field_values={"a": "", "b": "1"})
    strategy = InferExtractionCompleteness(required_fields=["a", "b"])

    result = strategy.execute(context)
    assert result.output["missing_required_fields"] == ["a"]
