from types import SimpleNamespace

from strategy.base import StrategyResult


def test_merge_updates_applies_updates_and_errors():
    context = SimpleNamespace(errors=[], applied={}, metadata={})

    def apply_updates(updates):
        context.applied.update(updates)

    def add_error(msg):
        context.errors.append(msg)

    context.apply_updates = apply_updates
    context.add_error = add_error

    result = StrategyResult(output=123, context_updates={"a": 1}, errors=["oops"])
    result.merge_updates(context)

    assert context.applied["a"] == 1
    assert context.errors == ["oops"]
