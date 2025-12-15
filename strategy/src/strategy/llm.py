from __future__ import annotations

import json
import os
from typing import Any, Callable, Dict, Mapping, Optional

from strategy.k1 import DOC1_FIELD_TEMPLATE, FIELD_KEYS, load_document_values
from strategy.models.k1.pydantic_model import map_to_generic_lines

from .base import BaseStrategy, StrategyError, StrategyResult


class OpenRouterExtractK1(BaseStrategy[Dict[str, str]]):
    """Extract K-1 field values using OpenRouter."""

    def __init__(
        self,
        *,
        model: str = "openai/gpt-4o-mini",
        api_key: Optional[str] = None,
        base_url: str = "https://openrouter.ai/api/v1/chat/completions",
        field_defaults: Optional[Mapping[str, str]] = None,
        request_func: Optional[Callable[[str, str, Mapping[str, Any]], Mapping[str, Any]]] = None,
    ):
        super().__init__(name="OpenRouterExtractK1", version="v1", activity="extract_fields")
        self.model = model
        self.api_key = api_key
        self.base_url = base_url
        self.field_defaults = field_defaults or DOC1_FIELD_TEMPLATE
        self.request_func = request_func or self._default_request

    def _build_messages(self, markdown: str) -> list[dict[str, str]]:
        field_list = ", ".join(FIELD_KEYS)
        system_prompt = (
            "Extract U.S. partnership Schedule K-1 values as JSON. "
            "Return a flat object where keys are provided field names and values are strings. "
            "Use '0' for missing numeric values and empty string for unknown text."
        )
        user_prompt = (
            f"Fields to extract: {field_list}\n\n"
            "Document markdown:\n"
            f"{markdown}"
        )
        return [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]

    def _default_request(
        self, api_key: str, base_url: str, payload: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        try:
            import requests
        except ImportError as exc:  # pragma: no cover - dependency guard
            raise StrategyError("requests is required for OpenRouterExtractK1") from exc

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        response = requests.post(
            base_url,
            headers=headers,
            json=payload,
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def _parse_response(self, raw: Mapping[str, Any]) -> Dict[str, str]:
        choices = raw.get("choices")
        if not choices:
            raise StrategyError("OpenRouter response missing choices")
        message = choices[0].get("message") or {}
        content = message.get("content")
        if not content:
            raise StrategyError("OpenRouter response missing content")
        try:
            data = json.loads(content)
        except json.JSONDecodeError as exc:
            raise StrategyError("OpenRouter content was not valid JSON") from exc
        if not isinstance(data, Mapping):
            raise StrategyError("OpenRouter content was not a JSON object")
        field_values = {key: str(value or "") for key, value in data.items()}
        for field, default in self.field_defaults.items():
            field_values.setdefault(field, default)
        return field_values

    def execute(self, context):
        if not context.parsed_markdown:
            raise StrategyError("parsed_markdown is required before LLM extraction")

        api_key = self.api_key or os.getenv("OPENROUTER_API_KEY")
        if not api_key:
            raise StrategyError("OPENROUTER_API_KEY is not configured")

        messages = self._build_messages(context.parsed_markdown)
        payload = {
            "model": self.model,
            "messages": messages,
            "response_format": {"type": "json_object"},
        }
        raw_response = self.request_func(api_key, self.base_url, payload)
        field_values = self._parse_response(raw_response)
        generic_lines = map_to_generic_lines(field_values)

        return StrategyResult(
            output=field_values,
            artifacts={
                "generic_lines": generic_lines.model_dump(),
                "openrouter_payload": {k: v for k, v in payload.items() if k != "messages"},
                "openrouter_response": raw_response,
            },
            context_updates={
                "field_values": field_values,
                "metadata": {**context.metadata, "generic_lines": generic_lines},
            },
        )


class MockOpenRouterExtractK1(BaseStrategy[Dict[str, str]]):
    """Mock OpenRouter extractor for tests and offline runs."""

    def __init__(
        self,
        *,
        mock_values: Optional[Mapping[str, str]] = None,
    ):
        super().__init__(name="MockOpenRouterExtractK1", version="v1", activity="extract_fields")
        self.mock_values = dict(mock_values) if mock_values else None

    def execute(self, context):
        pdf_name = context.pdf_path.name
        values = dict(self.mock_values) if self.mock_values else load_document_values(pdf_name)
        generic_lines = map_to_generic_lines(values)

        return StrategyResult(
            output=values,
            artifacts={
                "generic_lines": generic_lines.model_dump(),
                "source": "mock_openrouter",
            },
            context_updates={
                "field_values": values,
                "metadata": {**context.metadata, "generic_lines": generic_lines},
            },
        )
