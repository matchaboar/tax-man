from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, Optional

from fastapi.encoders import jsonable_encoder

from .models import WorkflowRunResult


DEFAULT_LOG_DIR = Path(__file__).resolve().parents[2] / "run-logs"


def _coerce_filename(name: Optional[str]) -> str:
    if not name:
        return "last_run.json"
    safe = "".join(ch for ch in name if ch.isalnum() or ch in ("-", "_", "."))
    return safe or "last_run.json"


def write_run_log(
    *,
    result: WorkflowRunResult,
    config: Mapping[str, object],
    filename: Optional[str] = None,
    log_dir: Path = DEFAULT_LOG_DIR,
) -> Path:
    """Persist a single workflow run to a JSON file for quick inspection."""
    log_dir.mkdir(parents=True, exist_ok=True)
    path = log_dir / _coerce_filename(filename)
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "config": jsonable_encoder(config),
        "result": jsonable_encoder(result.model_dump()),
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


def log_to_wandb(
    *,
    result: WorkflowRunResult,
    config: Mapping[str, object],
    project: Optional[str] = None,
    entity: Optional[str] = None,
    run_name: Optional[str] = None,
) -> Optional[str]:
    """Send a workflow run summary to W&B if configured. Returns run URL when available."""
    try:
        import wandb
    except ImportError:
        return None

    if os.getenv("WANDB_DISABLED") == "true" or not os.getenv("WANDB_API_KEY"):
        return None

    encoded_result = jsonable_encoder(result.model_dump())
    encoded_config = jsonable_encoder(config)

    run = wandb.init(
        project=project or "tax-man",
        entity=entity,
        name=run_name,
        config=encoded_config,
        reinit=True,
    )
    run.summary.update(
        {
            "succeeded": encoded_result.get("succeeded", False),
            "error_count": len(encoded_result.get("errors") or []),
            "field_value_count": len(encoded_result.get("field_values") or {}),
            "numeric_value_count": len(encoded_result.get("numeric_values") or {}),
        }
    )
    run.log({"artifacts": encoded_result.get("artifacts", {})})
    run.finish()
    return getattr(run, "url", None)
