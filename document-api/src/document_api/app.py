from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Literal, Optional
from uuid import uuid4

from fastapi import Depends, FastAPI, File, HTTPException, UploadFile, status
from fastapi.responses import HTMLResponse

from .models import DocumentListResponse, DocumentRecord, WorkflowDebugRecord
from .store import InMemoryDocumentStore
from .workflow_runner import WorkflowRunner, run_k1_workflow


app = FastAPI(
    title="Document API",
    version="0.1.0",
    description="Upload tax PDFs, run the K-1 workflow, and fetch parsed results.",
    docs_url="/docs",
    redoc_url="/redoc",
)

# Simple module-level dependencies so tests can override them.
document_store = InMemoryDocumentStore()


def get_document_store() -> InMemoryDocumentStore:
    return document_store


def get_workflow_runner() -> WorkflowRunner:
    return run_k1_workflow


def _validate_upload(file: UploadFile) -> None:
    if not file.filename:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="filename is required")
    if file.content_type not in (None, "application/pdf", "application/octet-stream"):
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE, detail="Only PDF uploads are supported"
        )


@app.get(
    "/documents",
    response_model=DocumentListResponse,
    summary="List available document IDs",
    tags=["documents"],
)
def list_documents(store: InMemoryDocumentStore = Depends(get_document_store)) -> DocumentListResponse:
    return DocumentListResponse(document_ids=store.list_ids())


@app.get(
    "/documents/{document_id}",
    response_model=DocumentRecord,
    summary="Get parsed result for a document ID",
    tags=["documents"],
)
def get_document(
    document_id: str, store: InMemoryDocumentStore = Depends(get_document_store)
) -> DocumentRecord:
    record = store.get(document_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Document not found")
    return record


def _render_workflow_view(document_id: Optional[str] = None) -> HTMLResponse:
    page = f"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>Workflow Viewer</title>
  <style>
    body {{
      font-family: Arial, sans-serif;
      max-width: 1200px;
      margin: 2rem auto;
      padding: 0 1rem;
      background: #f6f7fb;
      color: #222;
    }}
    header {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 1rem;
      flex-wrap: wrap;
    }}
    input[type=text] {{
      padding: 0.5rem;
      min-width: 280px;
      border: 1px solid #ccc;
      border-radius: 6px;
    }}
    button {{
      padding: 0.5rem 1rem;
      border: none;
      border-radius: 6px;
      background: #2d6cdf;
      color: #fff;
      cursor: pointer;
    }}
    button:disabled {{
      opacity: 0.5;
      cursor: not-allowed;
    }}
    section {{
      background: #fff;
      margin-top: 1rem;
      padding: 1rem;
      border-radius: 8px;
      box-shadow: 0 1px 3px rgba(0,0,0,0.08);
    }}
    .steps {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 0.75rem;
    }}
    .card {{
      border: 1px solid #e5e7eb;
      border-radius: 8px;
      padding: 0.75rem;
      background: #fafafa;
    }}
    pre {{
      white-space: pre-wrap;
      word-break: break-word;
      background: #f3f4f6;
      padding: 0.5rem;
      border-radius: 6px;
      max-height: 240px;
      overflow: auto;
    }}
    .error {{ color: #c1121f; font-weight: 600; }}
    .muted {{ color: #555; font-size: 0.9rem; }}
  </style>
</head>
<body>
  <header>
    <div>
      <h1 style="margin:0;">Workflow Viewer</h1>
      <p class="muted" style="margin:0;">Inspect PDF, step inputs/outputs, and response body.</p>
    </div>
    <div>
      <input id="docId" type="text" placeholder="Document ID" value="{document_id or ''}" />
      <button id="loadBtn">Load</button>
    </div>
  </header>

  <section id="summary">
    <strong>Summary:</strong> Enter a document ID and click Load.
  </section>

  <section id="pdf" style="display:none;"></section>

  <section id="documents">
    <h3 style="margin-top:0;">Available Documents</h3>
    <div id="docList" class="muted">Loading list…</div>
  </section>

  <section id="steps" style="display:none;">
    <h3 style="margin-top:0;">Workflow Steps</h3>
    <div id="stepsList"></div>
  </section>

  <section id="response" style="display:none;">
    <h3 style="margin-top:0;">Response Body</h3>
    <pre id="responseJson"></pre>
  </section>

  <script>
    const docInput = document.getElementById('docId');
    const loadBtn = document.getElementById('loadBtn');
    const summaryEl = document.getElementById('summary');
    const pdfEl = document.getElementById('pdf');
    const docList = document.getElementById('docList');
    const stepsSection = document.getElementById('steps');
    const stepsList = document.getElementById('stepsList');
    const responseSection = document.getElementById('response');
    const responseJson = document.getElementById('responseJson');

    function renderSummary(data) {{
      const errors = data.response_body?.errors || [];
      summaryEl.innerHTML = `
        <div><strong>PDF:</strong> ${{data.pdf_filename}}</div>
        <div><strong>Run ID:</strong> ${{data.id}}</div>
        <div><strong>Succeeded:</strong> ${{data.response_body?.succeeded}}</div>
        <div><strong>Errors:</strong> ${{errors.length ? '<span class="error">' + errors.join('; ') + '</span>' : 'None'}}</div>
      `;
    }}

    function renderPdf(data) {{
      const src = `data:application/pdf;base64,${{data.pdf_base64}}`;
      pdfEl.style.display = 'block';
      pdfEl.innerHTML = `
        <h3 style="margin-top:0;">PDF</h3>
        <a href="${{src}}" download="${{data.pdf_filename}}">Download PDF</a>
        <div style="margin-top:0.5rem;">
          <iframe src="${{src}}" style="width:100%; height:500px; border:1px solid #e5e7eb;"></iframe>
        </div>
      `;
    }}

    function renderSteps(data) {{
      stepsList.innerHTML = '';
      (data.steps || []).forEach((step, idx) => {{
        const errors = step.errors && step.errors.length ? `<div class="error">Errors: ${{step.errors.join('; ')}}</div>` : '';
        const details = document.createElement('details');
        details.className = 'card';
        if (idx === 0) details.open = true;
        const summary = document.createElement('summary');
        summary.innerHTML = `<strong>${{step.name}}</strong> · ${{step.strategy_name}} (${{step.strategy_version}})`;
        details.appendChild(summary);
        const body = document.createElement('div');
        body.innerHTML = `
          ${{errors}}
          <div class="muted">Input context</div>
          <pre>${{JSON.stringify(step.input_context, null, 2)}}</pre>
          <div class="muted">Output</div>
          <pre>${{JSON.stringify(step.output, null, 2)}}</pre>
          <div class="muted">Artifacts</div>
          <pre>${{JSON.stringify(step.artifacts, null, 2)}}</pre>
          <div class="muted">Post context</div>
          <pre>${{JSON.stringify(step.post_context, null, 2)}}</pre>
        `;
        details.appendChild(body);
        stepsList.appendChild(details);
      }});
      stepsSection.style.display = data.steps?.length ? 'block' : 'none';
    }}

    function renderResponse(data) {{
      responseJson.textContent = JSON.stringify(data.response_body, null, 2);
      responseSection.style.display = 'block';
    }}

    async function loadDocumentList() {{
      try {{
        const res = await fetch('/documents');
        if (!res.ok) throw new Error('Failed to load documents');
        const data = await res.json();
        const ids = data.document_ids || [];
        if (!ids.length) {{
          docList.innerHTML = 'No documents yet. Upload via POST /documents.';
          return;
        }}
        docList.innerHTML = ids
          .map((id) => `<a href="/workflow/view?document_id=${{encodeURIComponent(id)}}">${{id}}</a>`)
          .join('<br>');
      }} catch (err) {{
        docList.innerHTML = `<span class="error">${{err.message}}</span>`;
      }}
    }}

    async function loadWorkflow() {{
      const id = docInput.value.trim();
      if (!id) return;
      loadBtn.disabled = true;
      summaryEl.textContent = 'Loading...';
      pdfEl.style.display = 'none';
      stepsSection.style.display = 'none';
      responseSection.style.display = 'none';
      try {{
        const res = await fetch(`/workflow/${{id}}`);
        if (!res.ok) throw new Error(`Request failed (${{res.status}})`);
        const data = await res.json();
        renderSummary(data);
        renderPdf(data);
        renderSteps(data);
        renderResponse(data);
      }} catch (err) {{
        summaryEl.innerHTML = `<span class="error">${{err.message}}</span>`;
      }} finally {{
        loadBtn.disabled = false;
      }}
    }}

    loadBtn.addEventListener('click', loadWorkflow);
    docInput.addEventListener('keydown', (e) => {{
      if (e.key === 'Enter') loadWorkflow();
    }});

    loadDocumentList();
    if (docInput.value) {{
      loadWorkflow();
    }}
  </script>
</body>
</html>
    """
    return HTMLResponse(page)


@app.get(
    "/workflow/view",
    response_class=HTMLResponse,
    summary="Lightweight UI to explore workflow runs",
    tags=["workflow"],
)
def workflow_view(document_id: Optional[str] = None) -> HTMLResponse:
    return _render_workflow_view(document_id)


@app.get(
    "/workflow/ui",
    response_class=HTMLResponse,
    include_in_schema=False,
)
def workflow_ui(document_id: Optional[str] = None) -> HTMLResponse:
    return _render_workflow_view(document_id)


@app.get(
    "/workflow/{document_id}",
    response_model=WorkflowDebugRecord,
    summary="Inspect workflow inputs/outputs and PDF",
    tags=["workflow"],
)
def get_workflow(
    document_id: str, store: InMemoryDocumentStore = Depends(get_document_store)
) -> WorkflowDebugRecord:
    record = store.get_debug(document_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Workflow not found")
    return record


@app.post(
    "/documents",
    response_model=DocumentRecord,
    status_code=status.HTTP_201_CREATED,
    summary="Upload a PDF and parse it",
    tags=["documents"],
    response_description="Parsed document payload including workflow artifacts",
)
async def create_document(
    *,
    file: UploadFile = File(...),
    workflow: Optional[Literal["regex", "llm"]] = None,
    workflow_config: Optional[str] = None,
    use_mock_parser: Optional[bool] = None,
    use_mock_llm: Optional[bool] = None,
    llm_model: Optional[str] = None,
    strategy_version: Optional[str] = None,
    required_fields: Optional[str] = None,
    enable_wandb: bool = False,
    wandb_project: Optional[str] = None,
    wandb_entity: Optional[str] = None,
    wandb_run_name: Optional[str] = None,
    write_log_file: bool = False,
    log_filename: Optional[str] = None,
    store: InMemoryDocumentStore = Depends(get_document_store),
    workflow_runner: WorkflowRunner = Depends(get_workflow_runner),
) -> DocumentRecord:
    _validate_upload(file)
    payload = await file.read()
    if not payload:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Uploaded file is empty")

    with tempfile.TemporaryDirectory() as tmp_dir:
        filename = Path(file.filename).name or "document.pdf"
        pdf_path = Path(tmp_dir) / filename
        pdf_path.write_bytes(payload)
        parsed_result = workflow_runner(
            pdf_path=pdf_path,
            workflow_config=workflow_config,
            workflow=workflow,
            use_mock_parser=use_mock_parser,
            use_mock_llm=use_mock_llm,
            llm_model=llm_model,
            required_fields=[field.strip() for field in required_fields.split(",")] if required_fields else None,
            strategy_version=strategy_version,
            enable_wandb=enable_wandb,
            wandb_project=wandb_project,
            wandb_entity=wandb_entity,
            wandb_run_name=wandb_run_name,
            write_log_file=write_log_file,
            log_filename=log_filename,
        )

    document_id = uuid4().hex
    record = DocumentRecord(id=document_id, **parsed_result.model_dump())
    store.save_debug(
        document_record=record,
        pdf_bytes=payload,
        pdf_filename=filename,
        trace=parsed_result.trace,
    )
    return record
