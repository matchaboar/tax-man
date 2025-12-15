import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

import strategy.k1.regex_extractor as rx
from strategy.base import BaseStrategy, StrategyResult
from strategy.extraction import ExtractNumericValues, ExtractRegexK1
from strategy.llm import OpenRouterExtractK1
from strategy.parse import ParsePdfToDatalabMarkdown, MockParsePdfToDatalabMarkdown
from workflow.context import WorkflowContext
from document_api import telemetry
from document_api.models import WorkflowRunResult


def test_base_strategy_call_and_execute_not_implemented():
    base = BaseStrategy(name="b", version="v", activity="a")
    with pytest.raises(NotImplementedError):
        base.execute(None)  # type: ignore[arg-type]

    class Impl(BaseStrategy[int]):
        def execute(self, context):
            return StrategyResult(output=1)

    impl = Impl(name="b", version="v", activity="a")
    assert impl(None).output == 1  # type: ignore[arg-type]


def test_extract_numeric_values_uses_base_values(monkeypatch):
    class FakeExtractor:
        def __init__(self, *_):
            self.base_values = {"x": "10"}
            self.table_values = {}
            self.table_contexts = {}

    monkeypatch.setattr("strategy.extraction.ParsedK1RegexExtractor", FakeExtractor)
    context = WorkflowContext(pdf_path=Path("doc.pdf"), parsed_markdown="md")
    result = ExtractNumericValues().execute(context)
    assert result.output.numeric_fields["x"] == "10"


def test_extract_regex_loads_custom_config(tmp_path):
    cfg = tmp_path / "fields.yaml"
    cfg.write_text("fields:\n  a: b\n")
    strategy = ExtractRegexK1(strategy_config_path=cfg)
    assert strategy._load_config()["a"] == "b"


def test_llm_default_request_and_parse_errors(monkeypatch, tmp_path):
    class FakeResponse:
        def __init__(self):
            self.called = False
        def raise_for_status(self):
            self.called = True
        def json(self):
            return {"choices": [{"message": {"content": json.dumps({"a": 1})}}]}

    class FakeRequests:
        def __init__(self):
            self.last = None
        def post(self, base_url, headers=None, json=None, timeout=None):
            self.last = (base_url, headers, json, timeout)
            return FakeResponse()

    fake_requests = FakeRequests()
    monkeypatch.setitem(sys.modules, "requests", fake_requests)

    extractor = OpenRouterExtractK1(api_key="k")
    payload = {"x": 1}
    resp = extractor._default_request("k", "http://example.com", payload)
    assert "choices" in resp

    with pytest.raises(Exception):
        extractor._parse_response({})
    with pytest.raises(Exception):
        extractor._parse_response({"choices": [{"message": {"content": ""}}]})
    with pytest.raises(Exception):
        extractor._parse_response({"choices": [{"message": {"content": "not-json"}}]})
    with pytest.raises(Exception):
        extractor._parse_response({"choices": [{"message": {"content": json.dumps([1,2])}}]})

    ctx = WorkflowContext(pdf_path=tmp_path / "doc.pdf")
    with pytest.raises(Exception):
        extractor.execute(ctx)


def test_parse_convert_error_and_missing_markdown(monkeypatch, tmp_path):
    pdf = tmp_path / "file.pdf"
    pdf.write_text("pdf")
    context = WorkflowContext(pdf_path=pdf)

    class BadClient:
        def convert(self, *_args, **_kwargs):
            raise RuntimeError("boom")

    strategy = ParsePdfToDatalabMarkdown(api_key="k", client_factory=lambda *_: BadClient())
    with pytest.raises(Exception):
        strategy.execute(context)

    class NoMarkdownClient:
        def convert(self, *_args, **_kwargs):
            return SimpleNamespace(markdown=None)

    strategy = ParsePdfToDatalabMarkdown(api_key="k", client_factory=lambda *_: NoMarkdownClient())
    with pytest.raises(Exception):
        strategy.execute(context)

    missing_context = WorkflowContext(pdf_path=tmp_path / "missing.pdf")
    mock_strategy = MockParsePdfToDatalabMarkdown(fixture_root=tmp_path)
    with pytest.raises(Exception):
        mock_strategy.execute(missing_context)


def test_parse_mock_fixture_root_selection(monkeypatch, tmp_path):
    target = tmp_path / "strategy" / "test" / "fixtures" / "MockParsePdfToMarkdown"
    target.mkdir(parents=True)
    strategy = MockParsePdfToDatalabMarkdown(fixture_root=target)
    assert strategy.fixture_root == target


def test_regex_cache_edge_cases(monkeypatch, tmp_path):
    monkeypatch.setattr(rx, "BRUTE_FORCE_CACHE_PATH", tmp_path / "cache.json")
    monkeypatch.setattr(rx, "_BRUTE_FORCE_PATTERN_CACHE", {})
    # Missing file
    assert rx._load_brute_force_pattern_cache() == {}
    # Bad json
    rx.BRUTE_FORCE_CACHE_PATH.write_text("not-json")
    assert rx._load_brute_force_pattern_cache() == {}
    # Wrong version
    rx.BRUTE_FORCE_CACHE_PATH.write_text(json.dumps({"version": 0, "patterns": {}}))
    assert rx._load_brute_force_pattern_cache() == {}
    # Patterns not dict
    rx.BRUTE_FORCE_CACHE_PATH.write_text(json.dumps({"version": rx.BRUTE_FORCE_CACHE_VERSION, "patterns": []}))
    assert rx._load_brute_force_pattern_cache() == {}
    # Entries not list
    rx.BRUTE_FORCE_CACHE_PATH.write_text(json.dumps({"version": rx.BRUTE_FORCE_CACHE_VERSION, "patterns": {"a": {}}}))
    assert rx._load_brute_force_pattern_cache() == {}


def test_regex_brute_force_helpers(monkeypatch):
    extractor = rx.ParsedK1RegexExtractor("text")
    extractor.brute_force_cache["field"] = "cached"
    assert rx._brute_force_strategy("field")(extractor) == "cached"

    snippets = extractor._gather_brute_force_snippets("field_name")
    assert snippets

    monkeypatch.setattr(rx, "_ensure_brute_force_executor", lambda: SimpleNamespace(submit=lambda fn, arg: SimpleNamespace(result=lambda: [("p", True)])))
    patterns = extractor._generate_brute_force_patterns("field_name")
    assert patterns


def test_regex_table_desc_or_value_sets_context():
    extractor = rx.ParsedK1RegexExtractor("| A | desc | 1 |\n| H | desc <br> 2 | 3 |")
    extractor.table_descs["A"] = "line<br>4"
    strat = rx._table_desc_or_value_strategy("A", "field")
    value = strat(extractor)
    assert extractor.contexts["field"]
    assert value


def test_regex_line_value_with_summary_and_partnership_label():
    text = """
| line | Partnership's name | ABC |
SCHEDULE K-1 CURRENT YEAR NET INCOME
| DIVIDEND INCOME | 111 |
| 5 | interest | 222 |
"""
    extractor = rx.ParsedK1RegexExtractor(text)
    strat = rx._line_value_with_summary_strategy("5", "DIVIDEND INCOME", "field")
    assert strat(extractor)

    label_strat = rx._partnership_label_strategy("Partnership's name", "partner")
    assert label_strat(extractor) == "ABC"


def test_workflow_config_errors(tmp_path):
    from workflow import config as wf

    bad_body = tmp_path / "bad.yaml"
    bad_body.write_text(
        """
workflows:
  bad:
    - not-a-mapping
"""
    )
    with pytest.raises(wf.WorkflowConfigError):
        wf.load_workflow_configs(config_path=bad_body)

    empty = tmp_path / "empty.yaml"
    empty.write_text("workflows: {}\n")
    with pytest.raises(wf.WorkflowConfigError):
        wf.get_workflow_config(config_path=empty)

    missing_selected = tmp_path / "missing_selected.yaml"
    missing_selected.write_text(
        """
workflows:
  default:
    workflow: regex
    use_mock_parser: true
    use_mock_llm: true
    llm_model: model
    strategy_version: v1
"""
    )
    with pytest.raises(wf.WorkflowConfigError):
        wf.get_workflow_config(config_path=missing_selected)

    with pytest.raises(FileNotFoundError):
        wf.resolve_run_options(config_name="named", overrides={}, config_path=tmp_path / "nope.yaml")


def test_telemetry_import_error(monkeypatch):
    # Simulate wandb not installed
    real_import = __import__

    def fake_import(name, *args, **kwargs):
        if name == "wandb":
            raise ImportError("no wandb")
        return real_import(name, *args, **kwargs)

    # ensure branch where import proceeds
    fake_import("json")

    monkeypatch.setattr(sys.modules["builtins"], "__import__", fake_import)
    assert telemetry.log_to_wandb(result=WorkflowRunResult(), config={}) is None
