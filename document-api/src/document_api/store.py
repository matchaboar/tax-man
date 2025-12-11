from __future__ import annotations

import base64
from threading import Lock
from typing import Dict, Optional

from .models import DocumentRecord, WorkflowDebugRecord, WorkflowStepLog


class InMemoryDocumentStore:
    """Thread-safe in-memory storage for parsed document records."""

    def __init__(self) -> None:
        self._records: Dict[str, DocumentRecord] = {}
        self._debug_records: Dict[str, WorkflowDebugRecord] = {}
        self._lock = Lock()

    def save(self, record: DocumentRecord) -> DocumentRecord:
        with self._lock:
            self._records[record.id] = record
        return record

    def save_debug(
        self,
        *,
        document_record: DocumentRecord,
        pdf_bytes: bytes,
        pdf_filename: str,
        trace: list[dict],
    ) -> WorkflowDebugRecord:
        encoded_pdf = base64.b64encode(pdf_bytes).decode("ascii")
        debug_record = WorkflowDebugRecord(
            id=document_record.id,
            pdf_filename=pdf_filename,
            pdf_base64=encoded_pdf,
            steps=[WorkflowStepLog(**step) for step in trace],
            response_body=document_record,
        )
        with self._lock:
            self._records[document_record.id] = document_record
            self._debug_records[document_record.id] = debug_record
        return debug_record

    def get(self, document_id: str) -> Optional[DocumentRecord]:
        with self._lock:
            return self._records.get(document_id)

    def get_debug(self, document_id: str) -> Optional[WorkflowDebugRecord]:
        with self._lock:
            return self._debug_records.get(document_id)

    def list_ids(self) -> list[str]:
        with self._lock:
            return list(self._records.keys())

    def clear(self) -> None:
        with self._lock:
            self._records.clear()
            self._debug_records.clear()
