from __future__ import annotations

from typing import Any, Dict, List

from pydantic import BaseModel, Field


class WorkflowRunResult(BaseModel):
    """Normalized result produced by executing the K-1 workflow."""

    field_values: Dict[str, Any] = Field(default_factory=dict)
    numeric_values: Dict[str, Any] = Field(default_factory=dict)
    inference: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    artifacts: Dict[str, Any] = Field(default_factory=dict)
    trace: List[Dict[str, Any]] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)
    succeeded: bool = True


class DocumentRecord(WorkflowRunResult):
    """Stored representation of a parsed document."""

    id: str


class WorkflowStepLog(BaseModel):
    """One activity's before/after snapshot."""

    name: str
    strategy_name: str
    strategy_version: str
    input_context: Dict[str, Any] = Field(default_factory=dict)
    output: Any = None
    artifacts: Dict[str, Any] = Field(default_factory=dict)
    errors: List[str] = Field(default_factory=list)
    post_context: Dict[str, Any] = Field(default_factory=dict)


class WorkflowDebugRecord(BaseModel):
    """Full workflow run including PDF and per-step snapshots."""

    id: str
    pdf_filename: str
    pdf_base64: str
    steps: List[WorkflowStepLog] = Field(default_factory=list)
    response_body: DocumentRecord


class DocumentListResponse(BaseModel):
    """Simple listing of known document identifiers."""

    document_ids: List[str] = Field(default_factory=list)
