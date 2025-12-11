"""Evaluation helpers for comparing workflow/strategy runs on datasets."""

from .evaluation import (
    build_default_candidates,
    evaluate_candidates,
    load_dataset,
    summarize_results,
)

__all__ = [
    "build_default_candidates",
    "evaluate_candidates",
    "load_dataset",
    "summarize_results",
]
