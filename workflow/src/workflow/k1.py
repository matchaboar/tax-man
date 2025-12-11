from __future__ import annotations

from pathlib import Path
from typing import Iterable, Optional, Sequence

from strategy.extraction import (
    ExtractNumericValues,
    ExtractRegexK1,
    InferExtractionCompleteness,
)
from strategy.parse import MockParsePdfToDatalabMarkdown, ParsePdfToDatalabMarkdown
from strategy.llm import MockOpenRouterExtractK1, OpenRouterExtractK1
from .context import WorkflowContext
from .core import Activity, Workflow


def build_k1_workflow(
    *,
    pdf_path: Path,
    use_mock_parser: bool = True,
    required_fields: Optional[Iterable[str]] = None,
    strategy_version: str = "v1.0.0",
) -> tuple[Workflow, WorkflowContext]:
    """Assemble the default K-1 workflow with sensible defaults."""

    parse_strategy = (
        MockParsePdfToDatalabMarkdown() if use_mock_parser else ParsePdfToDatalabMarkdown()
    )
    activities: Sequence[Activity] = [
        Activity(name="parse", strategy=parse_strategy),
        Activity(name="extract_numbers", strategy=ExtractNumericValues()),
        Activity(
            name="extract_fields",
            strategy=ExtractRegexK1(version=strategy_version),
        ),
        Activity(
            name="infer",
            strategy=InferExtractionCompleteness(required_fields=required_fields),
        ),
    ]
    workflow = Workflow(name="k1-workflow", activities=activities)
    context = WorkflowContext(pdf_path=pdf_path)
    return workflow, context


def build_k1_llm_extract_workflow(
    *,
    pdf_path: Path,
    use_mock_parser: bool = True,
    use_mock_llm: bool = True,
    required_fields: Optional[Iterable[str]] = None,
    llm_model: str = "openai/gpt-4o-mini",
) -> tuple[Workflow, WorkflowContext]:
    """Assemble a K-1 workflow that uses OpenRouter for field extraction."""

    parse_strategy = (
        MockParsePdfToDatalabMarkdown() if use_mock_parser else ParsePdfToDatalabMarkdown()
    )
    extract_strategy = (
        MockOpenRouterExtractK1()
        if use_mock_llm
        else OpenRouterExtractK1(model=llm_model)
    )
    activities: Sequence[Activity] = [
        Activity(name="parse", strategy=parse_strategy),
        Activity(name="extract_numbers", strategy=ExtractNumericValues()),
        Activity(
            name="extract_fields",
            strategy=extract_strategy,
        ),
        Activity(
            name="infer",
            strategy=InferExtractionCompleteness(required_fields=required_fields),
        ),
    ]
    workflow = Workflow(name="k1-llm-extract", activities=activities)
    context = WorkflowContext(pdf_path=pdf_path)
    return workflow, context
