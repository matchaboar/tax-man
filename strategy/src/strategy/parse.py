from __future__ import annotations

import os
from pathlib import Path
from typing import Callable, Optional

from datalab_sdk import DatalabClient

from .base import BaseStrategy, StrategyError, StrategyResult


class ParsePdfToDatalabMarkdown(BaseStrategy[str]):
    """Call the Datalab API to convert a PDF to markdown."""

    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        client_factory: Optional[Callable[[str], object]] = None,
    ):
        super().__init__(name="ParsePdfToDatalabMarkdown", version="v1", activity="parse")
        self.api_key = api_key
        self._client_factory = client_factory or self._default_client_factory

    def _default_client_factory(self, api_key: str):
        try:
            from datalab_sdk import DatalabClient  # type: ignore
        except ImportError as exc:  # pragma: no cover - dependency guard
            raise StrategyError(
                "datalab-python-sdk is required for ParsePdfToDatalabMarkdown"
            ) from exc
        return DatalabClient(api_key=api_key)

    def _convert(self, client: DatalabClient, pdf_path: Path) -> str:
        try:
            from datalab_sdk.models import ConvertOptions  # type: ignore
        except ImportError as exc:  # pragma: no cover - dependency guard
            raise StrategyError(
                "datalab-python-sdk is required for ParsePdfToDatalabMarkdown"
            ) from exc

        try:
            options = ConvertOptions(output_format="markdown")
            result = client.convert(str(pdf_path), options=options)
        except Exception as exc:
            raise StrategyError(f"Datalab convert failed: {exc}") from exc

        markdown = getattr(result, "markdown", None)
        if not markdown:
            raise StrategyError("Datalab API response did not include markdown")
        return str(markdown)

    def execute(self, context):
        pdf_path = context.pdf_path
        if not pdf_path.exists():
            raise StrategyError(f"PDF not found: {pdf_path}")

        api_key = self.api_key or os.getenv("DATALAB_API_KEY")
        if not api_key:
            raise StrategyError("DATALAB_API_KEY is not configured")

        client = self._client_factory(api_key)
        markdown = self._convert(client, pdf_path)
        return StrategyResult(
            output=markdown,
            context_updates={"parsed_markdown": markdown},
            artifacts={"source": "datalab"},
        )


class MockParsePdfToDatalabMarkdown(BaseStrategy[str]):
    """Return canned markdown for a given PDF name."""

    def __init__(
        self,
        *,
        fixture_root: Optional[Path] = None,
    ):
        super().__init__(name="MockParsePdfToDatalabMarkdown", version="v1", activity="parse")
        package_root = Path(__file__).resolve().parent
        default_root = (
            package_root.parent / "test" / "fixtures" / "MockParsePdfToMarkdown"
        )
        if not default_root.exists():
            alt_root = package_root.parents[1] / "test" / "fixtures" / "MockParsePdfToMarkdown"
            workspace_root = package_root.parents[2]
            workspace_alt = workspace_root / "strategy" / "test" / "fixtures" / "MockParsePdfToMarkdown"
            if alt_root.exists():
                default_root = alt_root
            elif workspace_alt.exists():
                default_root = workspace_alt
        self.fixture_root = fixture_root or default_root

    def execute(self, context):
        pdf_path = context.pdf_path
        if not pdf_path.exists():
            raise StrategyError(f"PDF not found: {pdf_path}")

        fixture_name = pdf_path.with_suffix(".md").name
        fixture_path = self.fixture_root / "mock_markdown_response_body" / fixture_name
        if not fixture_path.exists():
            raise StrategyError(f"No mock markdown found for {fixture_name}")

        markdown = fixture_path.read_text(encoding="utf-8")
        return StrategyResult(
            output=markdown,
            context_updates={"parsed_markdown": markdown},
            artifacts={"source": "mock"},
        )
