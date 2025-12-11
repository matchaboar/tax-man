from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Generic, List, Mapping, Protocol, TypeVar


TResult = TypeVar("TResult")


class ContextProtocol(Protocol):
    def apply_updates(self, updates: Mapping[str, Any]) -> None:
        ...

    def add_error(self, message: str) -> None:
        ...


class StrategyError(RuntimeError):
    """Raised when a strategy cannot complete its work."""


@dataclass
class StrategyResult(Generic[TResult]):
    """Normalized strategy response payload."""

    output: TResult
    artifacts: Dict[str, Any] = field(default_factory=dict)
    context_updates: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)

    def merge_updates(self, context: ContextProtocol) -> None:
        """Apply any context updates declared by the strategy."""
        if self.context_updates:
            context.apply_updates(self.context_updates)
        if self.errors:
            for error in self.errors:
                context.add_error(error)


class BaseStrategy(Generic[TResult]):
    """Base interface for all strategies."""

    name: str
    version: str
    activity: str

    def __init__(self, *, name: str, version: str, activity: str):
        self.name = name
        self.version = version
        self.activity = activity

    def execute(self, context: ContextProtocol) -> StrategyResult[TResult]:
        raise NotImplementedError

    def __call__(self, context: ContextProtocol) -> StrategyResult[TResult]:
        return self.execute(context)
