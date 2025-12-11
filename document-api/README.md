# Document API

FastAPI service that parses K-1 tax PDFs by running the shared workflow package and returns structured field/numeric values. Swagger UI and OpenAPI JSON are available for quick inspection.

## Quick start

```bash
# From the workspace root (uv workspace already configured)
uv run uvicorn document_api.app:app --reload --host 0.0.0.0 --port 8000
```

API docs:
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`
- OpenAPI JSON: `http://localhost:8000/openapi.json`

## Endpoints

- `POST /documents`  
  Upload a PDF (`multipart/form-data` field `file`). Optional query params:  
  - `workflow` (`regex` | `llm`, default `regex`): choose regex extractor or LLM extractor.  
  - `use_mock_parser` (bool, default `true`): use fixture-backed parser; set to `false` to call the real parser (requires `DATALAB_API_KEY`).  
  - `use_mock_llm` (bool, default `true`): for `llm` workflow, set to `false` to hit OpenRouter (requires `OPENROUTER_API_KEY`).  
  - `llm_model` (str, default `openai/gpt-4o-mini`): OpenRouter model name.  
  - `strategy_version` (str, default `v1.0.0`)  
  - `required_fields` (comma-separated)  
  - Telemetry: `enable_wandb` (bool), `wandb_project`, `wandb_entity`, `wandb_run_name` (all optional), `write_log_file` (bool), `log_filename`  
  Response includes `id`, parsed `field_values`, `numeric_values`, `artifacts`, and `errors`. On success, `succeeded` is `true`.

- `GET /documents`  
  Returns `{"document_ids": [...]}` for stored runs.

- `GET /documents/{document_id}`  
  Returns the stored document result for the given id or `404` if missing.

- `GET /workflow/{document_id}`  
  Returns the uploaded PDF (base64), the full workflow step-by-step trace (input/output/strategy + artifacts), and the original response body for the document.

- `GET /workflow/view`  
  Simple HTML UI that lists all stored document IDs with links, lets you paste an ID, and shows the PDF, every workflow step (input/output/artifacts), and the response JSON.

### Workflow selection examples

- All mock: `workflow=regex&use_mock_parser=true` (default) or `workflow=llm&use_mock_parser=true&use_mock_llm=true`
- Real parse + regex: `workflow=regex&use_mock_parser=false`
- Real parse + real LLM: `workflow=llm&use_mock_parser=false&use_mock_llm=false&llm_model=openai/gpt-4o-mini`
- Mixed: `workflow=llm&use_mock_parser=true&use_mock_llm=false`

### Telemetry (W&B + file logging)

- Set `WANDB_API_KEY` (and optionally `WANDB_ENTITY`/`WANDB_PROJECT`) then pass `enable_wandb=true` to push a run summary to Weights & Biases.
- Add `write_log_file=true` (optionally `log_filename=custom.json`) to persist the request/response to `document-api/run-logs/`.

Example:

```bash
curl -X POST "http://localhost:8000/documents?workflow=llm&use_mock_parser=false&use_mock_llm=false&enable_wandb=true&write_log_file=true" \
  -H "Content-Type: multipart/form-data" \
  -F "file=@/path/to/k1.pdf"
```

## Testing

```bash
uv run pytest
```
