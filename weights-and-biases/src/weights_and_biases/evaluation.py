from __future__ import annotations

import csv
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple


def ensure_workspace_on_path(start: Optional[Path] = None) -> Path:
    """Add the monorepo root (containing strategy/workflow) to sys.path if missing."""
    search_root = start or Path(__file__).resolve()
    for parent in [search_root, *search_root.parents]:
        strategy_src = parent / "strategy" / "src"
        workflow_src = parent / "workflow" / "src"
        if strategy_src.exists() and workflow_src.exists():
            # Prefer src/ paths for complete packages; include repo root for project-level tools.
            for path in (strategy_src, workflow_src, parent):
                path_str = str(path)
                if path_str not in sys.path:
                    sys.path.insert(0, path_str)
            return parent
    raise RuntimeError("Could not locate workspace root containing strategy and workflow modules")


@dataclass
class DatasetSample:
    """One PDF plus its expected field values."""

    name: str
    pdf_path: Path
    ground_truth: Dict[str, str]


@dataclass
class CandidateWorkflow:
    """Workflow configuration to evaluate."""

    name: str
    description: str
    builder: Callable[[Path], Tuple[Any, Any]]  # returns (Workflow, WorkflowContext)
    requires_remote: bool = False


@dataclass
class SampleScore:
    sample_name: str
    matched: int
    total: int
    accuracy: float
    mismatches: Dict[str, Tuple[str, str]] = field(default_factory=dict)
    workflow_errors: List[str] = field(default_factory=list)

    @property
    def succeeded(self) -> bool:
        return not self.workflow_errors


@dataclass
class CandidateResult:
    candidate: CandidateWorkflow
    sample_scores: List[SampleScore] = field(default_factory=list)
    skipped_reason: Optional[str] = None

    @property
    def skipped(self) -> bool:
        return self.skipped_reason is not None

    @property
    def total_matched(self) -> int:
        return sum(score.matched for score in self.sample_scores)

    @property
    def total_fields(self) -> int:
        return sum(score.total for score in self.sample_scores)

    @property
    def accuracy(self) -> float:
        total_fields = self.total_fields
        return (self.total_matched / total_fields) if total_fields else 0.0


def normalize_value(value: Any) -> str:
    """Normalize values for comparison."""
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    text = (
        text.replace(",", "")
        .replace("$", "")
        .replace("%", "")
        .strip()
    )
    if text.startswith("(") and text.endswith(")"):
        inner = text[1:-1].strip()
        text = f"-{inner}" if inner else ""
    try:
        return str(int(text))
    except ValueError:
        return text


def values_match(actual: Any, expected: Any) -> bool:
    return normalize_value(actual) == normalize_value(expected)


def load_ground_truth(csv_path: Path) -> Dict[str, Dict[str, str]]:
    if not csv_path.exists():
        raise FileNotFoundError(f"Ground truth CSV not found: {csv_path}")

    with csv_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.reader(handle)
        header = next(reader, [])

        doc_names = [cell.strip() for cell in header[1:] if cell.strip()]
        truth: Dict[str, Dict[str, str]] = {doc: {} for doc in doc_names}

        for row in reader:
            if not row or not row[0].strip():
                continue
            field_name = row[0].strip()
            for idx, doc_name in enumerate(doc_names, start=1):
                value = row[idx] if idx < len(row) else ""
                truth[doc_name][field_name] = value.strip()
    return truth


def load_dataset(dataset_root: Path) -> List[DatasetSample]:
    ground_truth = load_ground_truth(dataset_root / "ground-truth" / "eval_set.csv")
    pdf_root = dataset_root / "input_pdf_docs"

    samples: List[DatasetSample] = []
    for doc_name, fields in ground_truth.items():
        pdf_path = pdf_root / doc_name
        samples.append(DatasetSample(name=doc_name, pdf_path=pdf_path, ground_truth=fields))
    return samples


def run_candidate_on_sample(candidate: CandidateWorkflow, sample: DatasetSample) -> SampleScore:
    try:
        workflow, context = candidate.builder(sample.pdf_path)
        workflow_result = workflow.run(context)
        extracted = context.field_values or {}
        workflow_errors = list(context.errors)
        if not workflow_result.succeeded and not workflow_errors:
            workflow_errors.append("workflow reported failure")
    except Exception as exc:
        total = len(sample.ground_truth)
        return SampleScore(
            sample_name=sample.name,
            matched=0,
            total=total,
            accuracy=0.0,
            mismatches={field: (expected, "") for field, expected in sample.ground_truth.items()},
            workflow_errors=[str(exc)],
        )

    mismatches: Dict[str, Tuple[str, str]] = {}
    matched = 0
    total = len(sample.ground_truth)
    for field_name, expected in sample.ground_truth.items():
        actual = extracted.get(field_name, "")
        if values_match(actual, expected):
            matched += 1
        else:
            mismatches[field_name] = (expected, str(actual) if actual is not None else "")

    accuracy = (matched / total) if total else 0.0
    return SampleScore(
        sample_name=sample.name,
        matched=matched,
        total=total,
        accuracy=accuracy,
        mismatches=mismatches,
        workflow_errors=workflow_errors,
    )


def evaluate_candidates(
    candidates: Sequence[CandidateWorkflow],
    samples: Sequence[DatasetSample],
    *,
    allow_remote: bool = False,
) -> List[CandidateResult]:
    results: List[CandidateResult] = []
    for candidate in candidates:
        if candidate.requires_remote and not allow_remote:
            results.append(
                CandidateResult(
                    candidate=candidate,
                    skipped_reason="remote parser disabled (pass --run-remote to attempt)",
                )
            )
            continue

        if candidate.requires_remote and not _has_datalab_api_key():
            results.append(
                CandidateResult(
                    candidate=candidate,
                    skipped_reason="DATALAB_API_KEY is not configured",
                )
            )
            continue

        sample_scores = [run_candidate_on_sample(candidate, sample) for sample in samples]
        results.append(CandidateResult(candidate=candidate, sample_scores=sample_scores))
    return results


def _has_datalab_api_key() -> bool:
    return bool(os.getenv("DATALAB_API_KEY"))


def build_default_candidates(strategy_version: str = "v1.0.0") -> List[CandidateWorkflow]:
    ensure_workspace_on_path()
    from workflow.k1 import build_k1_workflow

    return [
        CandidateWorkflow(
            name="k1-mock",
            description=f"Mock parser + regex extractor ({strategy_version})",
            builder=lambda pdf_path: build_k1_workflow(
                pdf_path=pdf_path,
                use_mock_parser=True,
                strategy_version=strategy_version,
            ),
            requires_remote=False,
        ),
        CandidateWorkflow(
            name="k1-remote",
            description=f"Remote Datalab parser + regex extractor ({strategy_version})",
            builder=lambda pdf_path: build_k1_workflow(
                pdf_path=pdf_path,
                use_mock_parser=False,
                strategy_version=strategy_version,
            ),
            requires_remote=True,
        ),
    ]


def summarize_results(results: Sequence[CandidateResult]) -> str:
    lines: List[str] = []
    for result in results:
        lines.append(f"- {result.candidate.name}: {result.candidate.description}")
        if result.skipped:
            lines.append(f"  skipped: {result.skipped_reason}")
            continue

        lines.append(
            f"  overall accuracy: {result.accuracy:.2%} ({result.total_matched}/{result.total_fields} fields)"
        )
        for score in result.sample_scores:
            status = "ok" if score.succeeded else "errors"
            lines.append(
                f"    {score.sample_name}: {score.accuracy:.1%} ({score.matched}/{score.total}) [{status}]"
            )
            if score.workflow_errors:
                lines.append(f"      workflow errors: {', '.join(score.workflow_errors)}")
            if score.mismatches:
                preview = list(score.mismatches.items())[:3]
                mismatch_desc = "; ".join(
                    f"{field} expected '{expected}' got '{actual}'"
                    for field, (expected, actual) in preview
                )
                extra = "" if len(score.mismatches) <= 3 else f" (+{len(score.mismatches) - 3} more)"
                lines.append(f"      mismatches: {mismatch_desc}{extra}")
    return "\n".join(lines)
