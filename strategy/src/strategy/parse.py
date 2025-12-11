from __future__ import annotations

import os
from pathlib import Path
from typing import Callable, Optional

from .base import BaseStrategy, StrategyError, StrategyResult


class ParsePdfToDatalabMarkdown(BaseStrategy[str]):
    """Call the Datalab API to convert a PDF to markdown."""

    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        base_url: str = "https://api.datalab.to/convert/pdf-to-markdown",
        request_func: Optional[Callable[[bytes, str, str], str]] = None,
    ):
        super().__init__(name="ParsePdfToDatalabMarkdown", version="v1", activity="parse")
        self.api_key = api_key
        self.base_url = base_url
        self.request_func = request_func or self._default_request

    def _default_request(self, payload: bytes, api_key: str, base_url: str) -> str:
        try:
            import requests
        except ImportError as exc:  # pragma: no cover - dependency guard
            raise StrategyError(
                "requests is required for ParsePdfToDatalabMarkdown"
            ) from exc

        response = requests.post(
            base_url,
            headers={"Authorization": f"Bearer {api_key}"},
            files={"file": ("document.pdf", payload, "application/pdf")},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        if isinstance(data, dict):
            text = data.get("markdown") or data.get("data")
            if text:
                return str(text)
        raise StrategyError("Datalab API response did not include markdown")

    def execute(self, context):
        pdf_path = context.pdf_path
        if not pdf_path.exists():
            raise StrategyError(f"PDF not found: {pdf_path}")

        api_key = self.api_key or os.getenv("DATALAB_API_KEY")
        if not api_key:
            raise StrategyError("DATALAB_API_KEY is not configured")

        markdown = self.request_func(pdf_path.read_bytes(), api_key, self.base_url)
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
