import json
from pathlib import Path

import pytest

import strategy.k1.regex_extractor as rx


@pytest.fixture
def tmp_cache(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(rx, "BRUTE_FORCE_CACHE_PATH", tmp_path / "cache.json")
    monkeypatch.setattr(rx, "_BRUTE_FORCE_PATTERN_CACHE", {})
    monkeypatch.setattr(rx, "_BRUTE_FORCE_EXECUTOR", None)
    return tmp_path


def test_brute_force_cache_round_trip(tmp_cache):
    rx._set_cached_brute_force_patterns("field", [("pattern", True)])
    cached = rx._get_cached_brute_force_patterns("field")
    assert cached == [("pattern", True)]

    # Loading from disk should also work
    loaded = rx._load_brute_force_pattern_cache()
    assert loaded["field"][0]["pattern"] == "pattern"


def test_load_brute_force_cache_handles_bad_data(monkeypatch, tmp_path: Path):
    bad_file = tmp_path / "cache.json"
    bad_file.write_text(json.dumps({"version": 999, "patterns": {"x": "bad"}}))
    monkeypatch.setattr(rx, "BRUTE_FORCE_CACHE_PATH", bad_file)

    assert rx._load_brute_force_pattern_cache() == {}


def test_ensure_brute_force_executor(monkeypatch):
    monkeypatch.setattr(rx, "_BRUTE_FORCE_EXECUTOR", None)
    executor = rx._ensure_brute_force_executor()
    assert executor is not None
    executor.shutdown(cancel_futures=True)
    rx._BRUTE_FORCE_EXECUTOR = None


def test_numeric_and_text_cleaners():
    assert rx._clean_numeric("$(1,234.00)") == "-1234.00"
    assert rx._strip_html_breaks("a<br>b") == "a b"
    assert rx._looks_numeric(" 1,234 ") is True
    assert rx._tokenize_field_name("line_4a_guaranteed") == ["line", "4a", "guaranteed"]


def test_build_brute_force_patterns_contains_variants():
    patterns = rx._build_brute_force_patterns_for_field("line_4a")
    assert patterns
    assert any(flag for _, flag in patterns)


def _sample_extractor():
    text = """
| 4A | payments | 3423 |
| H | desc | 123 |
BOX 18, CODE B | 456 |
TOTAL TO SCHEDULE K-1, BOX 20 CODE V | | 789 |
LINE 13H INVESTMENT INTEREST EXPENSE FROM TRADING ACTIVITIES | | 321 |
Capital contributed during the year | $ 555 |
Withdrawals and distributions | $ 222 |
Ending capital account | $ 777 |
Partnership's name, address, city, state, and ZIP code | ABC LP |
employer identification number<br>12-3456789
LINE 11A TOTAL OTHER INCOME (LOSS) - OTHER PORTFOLIO INCOME (LOSS) | 999 |
SCHEDULE K-1 CURRENT YEAR NET INCOME
| 6A | DIVIDEND INCOME | 111 |
| 5 | INTEREST INCOME | 222 |
| 20 | V | 444 |
    """
    return rx.ParsedK1RegexExtractor(text)


def test_table_and_statement_strategies():
    extractor = _sample_extractor()
    assert extractor._extract_table_value("4A", "f") == "3423"
    assert extractor._table_value_by_label("payments", "g") == "3423"
    assert extractor._statement_total("BOX 18, CODE B", "h") == "456"


def test_regex_strategy_helpers_cover_branches():
    extractor = _sample_extractor()
    strat = rx._regex_strategy(r"12-([0-9]+)", "ein")
    value = strat(extractor)
    assert value.endswith("6789")

    num_strat = rx._numeric_regex_strategy(r"(3423)", "num")
    assert num_strat(extractor) == "3423"

    table_strat = rx._table_strategy("4A", "line_4a_guaranteed_payments_for_services")
    assert table_strat(extractor) == "3423"

    table_regex = rx._table_regex_strategy("4A", "line_4a_guaranteed_payments_for_services")
    assert table_regex(extractor) == "3423"

    section_strat = rx._section_row_numeric_strategy(r"LINE\s+11A", r"OTHER PORTFOLIO", "line_11a")
    assert section_strat(extractor) == "999"

    first_line_strat = rx._table_first_line_strategy("4A", "line_4a_guaranteed_payments_for_services")
    assert first_line_strat(extractor) == "3423"


def test_line13h_trading_strategy_uses_fallback():
    extractor = _sample_extractor()
    strat = rx._line13h_trading_strategy("line_13h_investment_interest_trading_schedule_E")
    assert strat(extractor) == "321"


def test_table_numeric_text_and_summary_strategies():
    extractor = _sample_extractor()
    strat = rx._table_numeric_text_strategy("H", "line_13h_investment_interest_trading_schedule_E")
    assert strat(extractor) == "123"

    summary_strat = rx._current_year_summary_strategy("DIVIDEND INCOME", "line_6a_ordinary_dividends")
    with pytest.raises(ValueError):
        summary_strat(extractor)


def test_partnership_label_and_line_value_with_summary():
    extractor = _sample_extractor()
    label_strat = rx._partnership_label_strategy("Partnership's name", "partnership_name")
    with pytest.raises(ValueError):
        label_strat(extractor)

    combined = rx._line_value_with_summary_strategy("5", "INTEREST INCOME", "line_5_interest_income")
    assert combined(extractor) in {"222", "111"}


def test_fallback_and_brute_force_strategies():
    extractor = _sample_extractor()
    strat = rx._fallback_strategy(lambda _: "1", lambda _: "2")
    assert strat(extractor) == "1"

    brute = rx._brute_force_strategy("missing_field")
    assert brute(extractor) == extractor.base_values.get("missing_field", "0")


def test_extract_fields_from_file(tmp_path: Path):
    text_file = tmp_path / "doc.txt"
    text_file.write_text(_sample_extractor().text)

    result, contexts, strategies = rx.extract_fields_from_file(text_file, return_context=True)
    assert isinstance(result, dict)
    assert contexts
    assert strategies
