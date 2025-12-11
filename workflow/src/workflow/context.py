from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional


@dataclass
class WorkflowContext:
    """Mutable state shared across activities in a workflow run."""

    pdf_path: Path
    parsed_markdown: Optional[str] = None
    numeric_values: Dict[str, str] = field(default_factory=dict)
    field_values: Dict[str, str] = field(default_factory=dict)
    inference: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)

    def apply_updates(self, updates: Mapping[str, Any]) -> None:
        """Apply updates to known fields, falling back to metadata for extras."""
        for key, value in updates.items():
            if hasattr(self, key):
                setattr(self, key, value)
            else:
                self.metadata[key] = value

    def add_error(self, message: str) -> None:
        self.errors.append(message)
