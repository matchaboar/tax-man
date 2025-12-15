from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Protocol, runtime_checkable

from fastapi.encoders import jsonable_encoder

from workflow.config import DEFAULT_WORKFLOW_OPTIONS, WorkflowConfigError, resolve_run_options
from workflow.core import WorkflowResult
from workflow.k1 import build_k1_llm_extract_workflow, build_k1_workflow

from .models import WorkflowRunResult
from .telemetry import log_to_wandb as record_wandb_run, write_run_log


DEFAULT_RUN_CONFIG = dict(DEFAULT_WORKFLOW_OPTIONS)


@runtime_checkable
class WorkflowRunner(Protocol):
    def __call__(
        self,
        *,
        pdf_path: Path,
        workflow: Optional[str] = None,
        workflow_config: Optional[str] = None,
        workflow_config_path: Optional[Path] = None,
        use_mock_parser: Optional[bool] = None,
        use_mock_llm: Optional[bool] = None,
        llm_model: Optional[str] = None,
        required_fields: Optional[Iterable[str]] = None,
        strategy_version: Optional[str] = None,
        enable_wandb: bool = False,
        wandb_project: Optional[str] = None,
        wandb_entity: Optional[str] = None,
        wandb_run_name: Optional[str] = None,
        write_log_file: bool = False,
        log_filename: Optional[str] = None,
    ) -> WorkflowRunResult: ...


def _as_mapping(value: Optional[Mapping]) -> dict:
    """Normalize mappings to plain dicts for JSON serialization."""
    encoded = jsonable_encoder(value) if value is not None else {}
    return dict(encoded) if isinstance(encoded, Mapping) else {}


def _resolve_run_config(
    *,
    workflow_config: Optional[str],
    workflow_config_path: Optional[Path],
    workflow: Optional[str],
    use_mock_parser: Optional[bool],
    use_mock_llm: Optional[bool],
    llm_model: Optional[str],
    required_fields: Optional[Iterable[str]],
    strategy_version: Optional[str],
) -> tuple[dict[str, Any], Optional[str]]:
    """Merge config defaults with explicit overrides."""
    overrides: dict[str, Any] = {}
    for key, value in (
        ("workflow", workflow),
        ("use_mock_parser", use_mock_parser),
        ("use_mock_llm", use_mock_llm),
        ("llm_model", llm_model),
        ("required_fields", required_fields),
        ("strategy_version", strategy_version),
    ):
        if value is not None:
            overrides[key] = value

    resolved, applied_name = resolve_run_options(
        config_name=workflow_config,
        overrides=overrides,
        config_path=workflow_config_path,
        defaults=DEFAULT_RUN_CONFIG,
    )
    if resolved.get("required_fields") is not None:
        resolved["required_fields"] = list(resolved["required_fields"])
    return resolved, applied_name or workflow_config


def _build_k1(
    *,
    workflow: str,
    pdf_path: Path,
    use_mock_parser: bool,
    use_mock_llm: bool,
    llm_model: str,
    required_fields: Optional[Iterable[str]],
    strategy_version: str,
):
    workflow_kind = (workflow or "regex").lower()
    if workflow_kind == "llm":
        return build_k1_llm_extract_workflow(
            pdf_path=pdf_path,
            use_mock_parser=use_mock_parser,
            use_mock_llm=use_mock_llm,
            required_fields=required_fields,
            llm_model=llm_model,
        )
    if workflow_kind == "regex":
        return build_k1_workflow(
            pdf_path=pdf_path,
            use_mock_parser=use_mock_parser,
            required_fields=required_fields,
            strategy_version=strategy_version,
        )
    raise ValueError(f"Unsupported workflow '{workflow_kind}'. Use 'regex' or 'llm'.")


def _context_snapshot(context) -> dict:
    """Lightweight, JSON-friendly view of the workflow context."""
    snapshot = {
        "pdf_path": str(getattr(context, "pdf_path", "")),
        "parsed_markdown": getattr(context, "parsed_markdown", None),
        "numeric_values": getattr(context, "numeric_values", None),
        "field_values": getattr(context, "field_values", None),
        "inference": getattr(context, "inference", None),
        "metadata": getattr(context, "metadata", None),
        "errors": getattr(context, "errors", None),
    }
    return jsonable_encoder(snapshot)


def _run_with_trace(workflow_obj, context):
    results = []
    trace = []
    for activity in workflow_obj.activities:
        input_ctx = _context_snapshot(context)
        result = activity.run(context)
        post_ctx = _context_snapshot(context)
        results.append(result)
        trace.append(
            {
                "name": result.name,
                "strategy_name": result.strategy_name,
                "strategy_version": result.strategy_version,
                "input_context": input_ctx,
                "output": result.output,
                "artifacts": result.artifacts,
                "errors": result.errors,
                "post_context": post_ctx,
            }
        )
    return WorkflowResult(context=context, activity_results=results), trace


def run_k1_workflow(
    *,
    pdf_path: Path,
    workflow: Optional[str] = None,
    workflow_config: Optional[str] = None,
    workflow_config_path: Optional[Path] = None,
    use_mock_parser: Optional[bool] = None,
    use_mock_llm: Optional[bool] = None,
    llm_model: Optional[str] = None,
    required_fields: Optional[Iterable[str]] = None,
    strategy_version: Optional[str] = None,
    enable_wandb: bool = False,
    wandb_project: Optional[str] = None,
    wandb_entity: Optional[str] = None,
    wandb_run_name: Optional[str] = None,
    write_log_file: bool = False,
    log_filename: Optional[str] = None,
) -> WorkflowRunResult:
    """Execute the K-1 workflow and normalize outputs for API responses."""
    try:
        resolved_config, applied_config = _resolve_run_config(
            workflow_config=workflow_config,
            workflow_config_path=workflow_config_path,
            workflow=workflow,
            use_mock_parser=use_mock_parser,
            use_mock_llm=use_mock_llm,
            llm_model=llm_model,
            required_fields=required_fields,
            strategy_version=strategy_version,
        )
    except (FileNotFoundError, WorkflowConfigError) as exc:
        return WorkflowRunResult(
            succeeded=False,
            errors=[str(exc)],
            field_values={},
            numeric_values={},
            inference={},
            metadata={"workflow_config": workflow_config or ""},
            artifacts={},
            trace=[],
        )

    run_config = dict(resolved_config)
    run_config["workflow_config"] = applied_config
    try:
        workflow_obj, context = _build_k1(
            workflow=resolved_config["workflow"],
            pdf_path=pdf_path,
            use_mock_parser=resolved_config["use_mock_parser"],
            use_mock_llm=resolved_config["use_mock_llm"],
            llm_model=resolved_config["llm_model"],
            required_fields=resolved_config.get("required_fields"),
            strategy_version=resolved_config["strategy_version"],
        )
    except Exception as exc:  # pragma: no cover - defensive
        result = WorkflowRunResult(
            succeeded=False,
            errors=[str(exc)],
            field_values={},
            numeric_values={},
            inference={},
            metadata={},
            artifacts={},
            trace=[],
        )
        if enable_wandb:
            try:
                record_wandb_run(
                    result=result,
                    config=run_config,
                    project=wandb_project,
                    entity=wandb_entity,
                    run_name=wandb_run_name,
                )
            except Exception:
                result.metadata["wandb_error"] = "Failed to log to W&B"
        if write_log_file:
            try:
                write_run_log(result=result, config=run_config, filename=log_filename)
            except Exception:
                result.metadata["log_error"] = "Failed to write log file"
        return result

    try:
        workflow_result, trace = _run_with_trace(workflow_obj, context)
    except Exception as exc:  # pragma: no cover - defensive
        context.add_error(str(exc))
        workflow_result = WorkflowResult(context=context, activity_results=[])
        trace = []

    errors = list(context.errors)
    artifacts = {}
    for activity in workflow_result.activity_results:
        artifacts[activity.name] = jsonable_encoder(
            {
                "artifacts": activity.artifacts,
                "errors": activity.errors,
                "strategy_name": activity.strategy_name,
                "strategy_version": activity.strategy_version,
            }
        )
        errors.extend(activity.errors)

    succeeded = workflow_result.succeeded and not errors
    metadata = _as_mapping(getattr(context, "metadata", None))
    metadata.setdefault("workflow", resolved_config["workflow"])
    if applied_config:
        metadata.setdefault("workflow_config", applied_config)
    return_result = WorkflowRunResult(
        succeeded=succeeded,
        errors=errors,
        field_values=_as_mapping(getattr(context, "field_values", None)),
        numeric_values=_as_mapping(getattr(context, "numeric_values", None)),
        inference=_as_mapping(getattr(context, "inference", None)),
        metadata=metadata,
        artifacts=artifacts,
        trace=trace,
    )
    if enable_wandb:
        try:
            wandb_url = record_wandb_run(
                result=return_result,
                config=run_config,
                project=wandb_project,
                entity=wandb_entity,
                run_name=wandb_run_name,
            )
            if wandb_url:
                return_result.metadata["wandb_run_url"] = wandb_url
        except Exception as exc:  # pragma: no cover - telemetry guard
            return_result.metadata["wandb_error"] = str(exc)
    if write_log_file:
        try:
            log_path = write_run_log(result=return_result, config=run_config, filename=log_filename)
            return_result.metadata["local_log_file"] = str(log_path)
        except Exception as exc:  # pragma: no cover - telemetry guard
            return_result.metadata["log_error"] = str(exc)
    return return_result
