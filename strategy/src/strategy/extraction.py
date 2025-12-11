from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, Mapping, Optional, Sequence

from strategy.models.k1.pydantic_model import GenericK1Lines, map_to_generic_lines
from strategy.k1.regex_extractor import (
    DOC1_FIELD_TEMPLATE,
    ParsedK1RegexExtractor,
    load_field_strategy_config,
)

from .base import BaseStrategy, StrategyError, StrategyResult


@dataclass
class NumericExtractionResult:
    table_values: Dict[str, str] = field(default_factory=dict)
    numeric_fields: Dict[str, str] = field(default_factory=dict)
    contexts: Dict[str, str] = field(default_factory=dict)


class ExtractNumericValues(BaseStrategy[NumericExtractionResult]):
    """Collect numeric values from the parsed document for later reference."""

    def __init__(self):
        super().__init__(name="ExtractNumericValues", version="v1", activity="extract_numbers")

    def execute(self, context):
        if not context.parsed_markdown:
            raise StrategyError("parsed_markdown is required before numeric extraction")

        extractor = ParsedK1RegexExtractor(context.parsed_markdown)
        numeric_fields: Dict[str, str] = {}
        for key, value in extractor.base_values.items():
            if value and value != "0":
                numeric_fields[key] = value
        numeric_fields.update(extractor.table_values)

        return StrategyResult(
            output=NumericExtractionResult(
                table_values=dict(extractor.table_values),
                numeric_fields=numeric_fields,
                contexts=dict(extractor.table_contexts),
            ),
            context_updates={"numeric_values": numeric_fields},
            artifacts={"table_contexts": extractor.table_contexts},
        )


class ExtractRegexK1(BaseStrategy[Dict[str, str]]):
    """Run the regex-based extractor for a given strategy config version."""

    def __init__(
        self,
        *,
        version: str = "v1.0.0",
        strategy_config_path: Optional[Path] = None,
        field_defaults: Optional[Mapping[str, str]] = None,
    ):
        super().__init__(name="ExtractRegexK1", version=version, activity="extract_fields")
        self.strategy_config_path = strategy_config_path
        self.field_defaults = field_defaults or DOC1_FIELD_TEMPLATE

    def _load_config(self) -> Mapping[str, str]:
        if self.strategy_config_path:
            return load_field_strategy_config(path=self.strategy_config_path)
        return load_field_strategy_config()

    def execute(self, context):
        if not context.parsed_markdown:
            raise StrategyError("parsed_markdown is required before regex extraction")

        extractor = ParsedK1RegexExtractor(
            context.parsed_markdown,
            field_defaults=self.field_defaults,
            strategy_config=self._load_config(),
        )
        field_values = extractor.extract()
        generic_lines = map_to_generic_lines(field_values)

        return StrategyResult(
            output=field_values,
            artifacts={
                "generic_lines": generic_lines.model_dump(),
                "used_strategies": extractor.used_strategies,
                "contexts": extractor.contexts,
            },
            context_updates={
                "field_values": field_values,
                "metadata": {**context.metadata, "generic_lines": generic_lines},
            },
        )


class InferExtractionCompleteness(BaseStrategy[Dict[str, Sequence[str]]]):
    """Lightweight sanity checks to flag missing critical fields."""

    def __init__(self, *, required_fields: Optional[Iterable[str]] = None):
        super().__init__(name="InferExtractionCompleteness", version="v1", activity="infer")
        self.required_fields = list(required_fields) if required_fields else [
            "partnership_name",
            "partnership_employer_identification_number",
        ]

    def execute(self, context):
        if not context.field_values:
            raise StrategyError("field_values is required before inference")

        missing = []
        for field in self.required_fields:
            value = context.field_values.get(field, "")
            if value in ("", "0", None):
                missing.append(field)

        warnings: Dict[str, Sequence[str]] = {}
        if missing:
            warnings["missing_required_fields"] = missing

        return StrategyResult(
            output=warnings,
            context_updates={"inference": warnings},
            artifacts={"required_fields": self.required_fields},
        )
