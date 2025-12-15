from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path
from typing import Sequence

import pandas as pd
import weave
from dotenv import load_dotenv

from pydantic import ConfigDict

from .evaluation import (
    CandidateWorkflow,
    build_default_candidates,
    ensure_workspace_on_path,
    load_dataset,
    normalize_value,
    values_match,
)


DEFAULT_DATASET = Path(__file__).resolve().parents[2] / "datasets" / "pdf-set-1"
DEFAULT_PROJECT = "tax-man/k1-test"


def load_env_from_file(env_path: Path) -> None:
    """Populate environment variables from a .env file (without overriding existing vars)."""
    if env_path.exists():
        load_dotenv(env_path, override=False)


class WorkflowModel(weave.Model):
    """Wraps a CandidateWorkflow so it can be evaluated with Weave."""

    candidate: CandidateWorkflow
    model_config = ConfigDict(arbitrary_types_allowed=True)

    def __init__(self, candidate: CandidateWorkflow):
        super().__init__(candidate=candidate)

    @weave.op
    def predict(self, pdf_path: str) -> dict[str, str]:
        workflow, context = self.candidate.builder(Path(pdf_path))
        workflow.run(context)
        return context.field_values or {}


@weave.op
def field_accuracy(ground_truth: dict, output: dict) -> dict:
    """Calculate accuracy and capture mismatches for Weave scoring."""
    if not isinstance(output, dict):
        return {"matched": 0, "total": len(ground_truth), "accuracy": 0.0, "mismatches": {}}

    mismatches = {}
    matched = 0
    total = len(ground_truth)
    for field, expected in ground_truth.items():
        actual = output.get(field, "")
        if values_match(actual, expected):
            matched += 1
        else:
            mismatches[field] = {
                "expected": normalize_value(expected),
                "actual": normalize_value(actual),
            }

    accuracy = (matched / total) if total else 0.0
    return {"matched": matched, "total": total, "accuracy": accuracy, "mismatches": mismatches}


def _build_dataframe(dataset_root: Path) -> pd.DataFrame:
    samples = load_dataset(dataset_root)
    return pd.DataFrame(
        [
            {
                "pdf_path": sample.pdf_path.as_posix(),
                "ground_truth": sample.ground_truth,
                "name": sample.name,
            }
            for sample in samples
        ],
        columns=["pdf_path", "ground_truth", "name"],
    )


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Weave/W&B evaluation for candidate workflows."
    )
    parser.add_argument(
        "--dataset",
        type=Path,
        default=DEFAULT_DATASET,
        help="Path to dataset root (default: datasets/pdf-set-1)",
    )
    parser.add_argument(
        "--project",
        default=DEFAULT_PROJECT,
        help="Weave/W&B project path (entity/project or project). Default: tax-man/k1-test",
    )
    parser.add_argument(
        "--entity",
        default=None,
        help="W&B entity/user (optional if provided in --project or via WANDB_ENTITY).",
    )
    parser.add_argument(
        "--name",
        default=None,
        help="Evaluation run name (defaults to candidate name + version).",
    )
    parser.add_argument(
        "--strategy-version",
        default="v1.0.0",
        help="Regex extraction strategy version (default: v1.0.0)",
    )
    parser.add_argument(
        "--run-remote",
        action="store_true",
        help="Include the remote parser candidate (requires DATALAB_API_KEY).",
    )
    parser.add_argument(
        "--publish-leaderboard",
        action="store_true",
        help="Publish a Weave leaderboard comparing candidates (field_accuracy mean).",
    )
    parser.add_argument(
        "--leaderboard-name",
        default=None,
        help="Leaderboard name (default: project + dataset name).",
    )
    parser.add_argument(
        "--leaderboard-description",
        default="Tax-man workflow extraction accuracy leaderboard.",
        help="Leaderboard description text.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = _parse_args(argv)
    repo_root = ensure_workspace_on_path()

    load_env_from_file(repo_root / ".env")
    if not os.getenv("WANDB_API_KEY"):
        print("WANDB_API_KEY is not set; set it in .env or your environment.")
        return

    dataset_root = args.dataset
    if not dataset_root.exists():
        raise SystemExit(f"Dataset path not found: {dataset_root}")

    # Resolve entity/project into a single project_name string for weave.init.
    project = args.project
    entity = args.entity or os.getenv("WANDB_ENTITY") or os.getenv("WANDB_USERNAME")
    if entity and "/" not in project:
        project_name = f"{entity}/{project}"
    else:
        project_name = project

    candidates = build_default_candidates(strategy_version=args.strategy_version)
    if not args.run_remote:
        candidates = [c for c in candidates if not c.requires_remote]

    df = _build_dataframe(dataset_root)
    weave.init(project_name)

    evaluations = []
    for candidate in candidates:
        run_name = args.name or f"{candidate.name}-{args.strategy_version}"
        model = WorkflowModel(candidate)
        evaluation = weave.Evaluation(
            name=run_name,
            dataset=weave.Dataset.from_pandas(df),
            scorers=[field_accuracy],
        )
        print(f"Running Weave evaluation for {candidate.name} (strategy {args.strategy_version})")
        asyncio.run(evaluation.evaluate(model))
        print(f"Completed run: {run_name}")
        evaluations.append((candidate.name, evaluation))

    if args.publish_leaderboard and evaluations:
        from weave.flow import leaderboard
        from weave.trace.ref_util import get_ref

        columns = []
        for candidate_name, evaluation in evaluations:
            columns.append(
                leaderboard.LeaderboardColumn(
                    evaluation_object_ref=get_ref(evaluation).uri(),
                    scorer_name="field_accuracy",
                    summary_metric_path="accuracy.mean",
                    name=f"{candidate_name} accuracy",
                    description="Mean field-level accuracy",
                )
            )

        lb_name = args.leaderboard_name or f"{project_name}-field-accuracy"
        spec = leaderboard.Leaderboard(
            name=lb_name,
            description=args.leaderboard_description,
            columns=columns,
        )
        ref = weave.publish(spec)
        print(f"Published leaderboard: {ref.uri()}")


if __name__ == "__main__":
    main()
