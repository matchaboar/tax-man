import importlib
import sys
from types import SimpleNamespace
from pathlib import Path

import pytest

import strategy.parse as parse
from strategy.parse import ParsePdfToDatalabMarkdown, MockParsePdfToDatalabMarkdown
from workflow.context import WorkflowContext


def test_parse_pdf_missing_file_raises(tmp_path: Path):
    context = WorkflowContext(pdf_path=tmp_path / "missing.pdf")
    strategy = ParsePdfToDatalabMarkdown(api_key="key", client_factory=lambda *_: None)

    with pytest.raises(Exception):
        strategy.execute(context)


def test_parse_pdf_missing_api_key_raises(tmp_path: Path):
    pdf = tmp_path / "file.pdf"
    pdf.write_text("pdf")
    context = WorkflowContext(pdf_path=pdf)
    strategy = ParsePdfToDatalabMarkdown(client_factory=lambda *_: None)

    with pytest.raises(Exception):
        strategy.execute(context)


def test_parse_pdf_success_with_stub_client(tmp_path: Path, monkeypatch):
    pdf = tmp_path / "file.pdf"
    pdf.write_text("pdf")

    class StubClient:
        def __init__(self, api_key=None):
            self.api_key = api_key

        def convert(self, *_args, **_kwargs):
            return SimpleNamespace(markdown="# heading")

    class StubConvertOptions:
        def __init__(self, output_format):
            self.output_format = output_format

    # Install fake datalab modules so _convert import paths resolve.
    monkeypatch.setitem(sys.modules, "datalab_sdk", SimpleNamespace(DatalabClient=StubClient))
    monkeypatch.setitem(sys.modules, "datalab_sdk.models", SimpleNamespace(ConvertOptions=StubConvertOptions))
    importlib.reload(parse)

    context = WorkflowContext(pdf_path=pdf)
    strategy = parse.ParsePdfToDatalabMarkdown(api_key="key")
    result = strategy.execute(context)
    result.merge_updates(context)

    assert result.output.startswith("# heading")
    assert context.parsed_markdown.startswith("# heading")


def test_default_client_factory_import_error(monkeypatch):
    # Force ImportError inside _default_client_factory by intercepting imports.
    real_import = __import__

    def fake_import(name, *args, **kwargs):
        if name == "datalab_sdk" or name.startswith("datalab_sdk"):
            raise ImportError("no datalab")
        return real_import(name, *args, **kwargs)

    fake_import("json")

    monkeypatch.setattr(sys.modules["builtins"], "__import__", fake_import)

    with pytest.raises(Exception):
        parse.ParsePdfToDatalabMarkdown(api_key="key")._default_client_factory("key")


def test_mock_parse_pdf_to_markdown_handles_missing_fixture(tmp_path: Path):
    pdf = tmp_path / "unknown.pdf"
    pdf.write_text("pdf")
    strategy = MockParsePdfToDatalabMarkdown(fixture_root=tmp_path)
    context = WorkflowContext(pdf_path=pdf)

    with pytest.raises(Exception):
        strategy.execute(context)
