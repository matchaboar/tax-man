# workflow

Lightweight orchestration layer for sequencing strategy activities and tracking shared context.

## Configured workflows

Workflows are assembled in `workflow/src/workflow/k1.py`.

### `k1-workflow` (regex extract)

```mermaid
flowchart TD
  Start([Start]) --> PDF[PDF path]

  PDF --> Parse{parse}
  Parse -->|MockParsePdfToDatalabMarkdown<br/>use_mock_parser=true| MD[parsed_markdown]
  Parse -->|ParsePdfToDatalabMarkdown<br/>use_mock_parser=false| MD

  MD --> Numbers["extract_numbers<br/>ExtractNumericValues<br/>updates: numeric_values"]
  Numbers --> Fields["extract_fields<br/>ExtractRegexK1<br/>strategy_version: strategy_version<br/>updates: field_values<br/>updates: metadata.generic_lines"]
  Fields --> Infer["infer<br/>InferExtractionCompleteness<br/>required_fields: required_fields<br/>updates: inference"]
  Infer --> Done([Done: WorkflowResult])
```

### `k1-llm-extract` (LLM extract)

```mermaid
flowchart TD
  Start([Start]) --> PDF[PDF path]

  PDF --> Parse{parse}
  Parse -->|MockParsePdfToDatalabMarkdown<br/>use_mock_parser=true| MD[parsed_markdown]
  Parse -->|ParsePdfToDatalabMarkdown<br/>use_mock_parser=false| MD

  MD --> Numbers["extract_numbers<br/>ExtractNumericValues<br/>updates: numeric_values"]
  Numbers --> Extract{extract_fields}
  Extract -->|MockOpenRouterExtractK1<br/>use_mock_llm=true| Fields["field_values<br/>updates: field_values<br/>updates: metadata.generic_lines"]
  Extract -->|OpenRouterExtractK1<br/>llm_model: llm_model<br/>use_mock_llm=false| Fields
  Fields --> Infer["infer<br/>InferExtractionCompleteness<br/>required_fields: required_fields<br/>updates: inference"]
  Infer --> Done([Done: WorkflowResult])
```
