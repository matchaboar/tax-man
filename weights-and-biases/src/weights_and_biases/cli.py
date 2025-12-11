from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from .evaluation import (
    build_default_candidates,
    ensure_workspace_on_path,
    evaluate_candidates,
    load_dataset,
    summarize_results,
)


DEFAULT_DATASET = Path(__file__).resolve().parents[2] / "datasets" / "pdf-set-1"


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Evaluate workflow/strategy combinations against the sample datasets."
    )
    parser.add_argument(
        "--dataset",
        type=Path,
        default=DEFAULT_DATASET,
        help="Path to dataset root (default: datasets/pdf-set-1)",
    )
    parser.add_argument(
        "--strategy-version",
        default="v1.0.0",
        help="Regex extraction strategy version (default: v1.0.0)",
    )
    parser.add_argument(
        "--run-remote",
        action="store_true",
        help="Attempt remote parser workflows (requires DATALAB_API_KEY).",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = _parse_args(argv)
    ensure_workspace_on_path()

    samples = load_dataset(args.dataset)
    candidates = build_default_candidates(strategy_version=args.strategy_version)
    results = evaluate_candidates(candidates, samples, allow_remote=args.run_remote)

    print(f"Dataset: {args.dataset}")
    print(summarize_results(results))


if __name__ == "__main__":
    main()
