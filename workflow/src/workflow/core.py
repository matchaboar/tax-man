from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Generic, List, Optional, Sequence, TypeVar

from strategy.base import BaseStrategy, StrategyError
from .context import WorkflowContext


TResult = TypeVar("TResult")


@dataclass
class ActivityResult(Generic[TResult]):
    """Result of running one activity."""

    name: str
    output: Optional[TResult]
    strategy_name: str
    strategy_version: str
    artifacts: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)

    @property
    def succeeded(self) -> bool:
        return not self.errors


@dataclass
class Activity(Generic[TResult]):
    """Wraps a strategy so it can participate in a workflow."""

    name: str
    strategy: BaseStrategy[TResult]

    def run(self, context: WorkflowContext) -> ActivityResult[TResult]:
        try:
            result = self.strategy.execute(context)
            result.merge_updates(context)
            output = result.output
            errors = list(result.errors)
            artifacts = dict(result.artifacts)
        except StrategyError as exc:
            context.add_error(str(exc))
            output = None
            artifacts = {}
            errors = [str(exc)]
        except Exception as exc:  # pragma: no cover - defensive
            context.add_error(str(exc))
            output = None
            artifacts = {}
            errors = [f"Unexpected error in {self.name}: {exc}"]

        return ActivityResult(
            name=self.name,
            output=output,
            strategy_name=self.strategy.name,
            strategy_version=self.strategy.version,
            artifacts=artifacts,
            errors=errors,
        )


@dataclass
class WorkflowResult:
    """Aggregated results from a workflow run."""

    context: WorkflowContext
    activity_results: List[ActivityResult[Any]] = field(default_factory=list)

    @property
    def succeeded(self) -> bool:
        return all(result.succeeded for result in self.activity_results)


class Workflow:
    """Sequential workflow runner."""

    def __init__(self, *, name: str, activities: Sequence[Activity[Any]]):
        self.name = name
        self.activities = list(activities)

    def run(self, context: WorkflowContext) -> WorkflowResult:
        results: List[ActivityResult[Any]] = []
        for activity in self.activities:
            results.append(activity.run(context))
        return WorkflowResult(context=context, activity_results=results)
