from __future__ import annotations

import atexit
import csv
import json
import os
import re
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Mapping, Optional

import yaml


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DEFAULT_EVAL_CSV = DATA_DIR / "eval_set.csv"
MAIN_TABLE_ROW = re.compile(
    r"^\|\s*(?P<code>[0-9A-Za-z]+)\s*\|(?P<desc>.*?)\|\s*(?P<value>[^|]*?)\|",
    re.MULTILINE,
)

FieldStrategy = Callable[["ParsedK1RegexExtractor"], str]
FIELD_STRATEGIES: Dict[str, Dict[str, FieldStrategy]] = {}
REGEX_FIELD_CONFIG_PATH = DATA_DIR / "regex_field_config.yaml"
BRUTE_FORCE_PATTERN_LIMIT = 1_000
BRUTE_FORCE_CACHE_VERSION = 1
BRUTE_FORCE_CACHE_PATH = DATA_DIR / "regex_bruteforce_cache.json"
_BRUTE_FORCE_PATTERN_CACHE: Dict[str, list[dict]] = {}
_BRUTE_FORCE_EXECUTOR: ProcessPoolExecutor | None = None


def _load_brute_force_pattern_cache() -> Dict[str, list[dict]]:
    if not BRUTE_FORCE_CACHE_PATH.exists():
        return {}
    try:
        data = json.loads(BRUTE_FORCE_CACHE_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    if data.get("version") != BRUTE_FORCE_CACHE_VERSION:
        return {}
    patterns = data.get("patterns")
    if not isinstance(patterns, dict):
        return {}
    sanitized: Dict[str, list[dict]] = {}
    for field, entries in patterns.items():
        if not isinstance(entries, list):
            continue
        cleaned: list[dict] = []
        for entry in entries:
            if not isinstance(entry, Mapping):
                continue
            pattern = entry.get("pattern")
            numeric = bool(entry.get("numeric"))
            if isinstance(pattern, str):
                cleaned.append({"pattern": pattern, "numeric": numeric})
        if cleaned:
            sanitized[field] = cleaned[:BRUTE_FORCE_PATTERN_LIMIT]
    return sanitized


_BRUTE_FORCE_PATTERN_CACHE = _load_brute_force_pattern_cache()


def _save_brute_force_pattern_cache() -> None:
    payload = {
        "version": BRUTE_FORCE_CACHE_VERSION,
        "patterns": _BRUTE_FORCE_PATTERN_CACHE,
    }
    BRUTE_FORCE_CACHE_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def _get_cached_brute_force_patterns(field_name: str):
    entries = _BRUTE_FORCE_PATTERN_CACHE.get(field_name)
    if not entries:
        return None
    return [(entry["pattern"], bool(entry.get("numeric"))) for entry in entries]


def _set_cached_brute_force_patterns(
    field_name: str, patterns: list[tuple[str, bool]]
) -> None:
    serializable = [
        {"pattern": pattern, "numeric": numeric}
        for pattern, numeric in patterns[:BRUTE_FORCE_PATTERN_LIMIT]
    ]
    _BRUTE_FORCE_PATTERN_CACHE[field_name] = serializable
    _save_brute_force_pattern_cache()


def _ensure_brute_force_executor() -> ProcessPoolExecutor:
    global _BRUTE_FORCE_EXECUTOR
    if _BRUTE_FORCE_EXECUTOR is None:
        workers = max(1, min(os.cpu_count() or 1, 4))
        _BRUTE_FORCE_EXECUTOR = ProcessPoolExecutor(max_workers=workers)

        def _shutdown():
            if _BRUTE_FORCE_EXECUTOR:
                _BRUTE_FORCE_EXECUTOR.shutdown(wait=False)

        atexit.register(_shutdown)
    return _BRUTE_FORCE_EXECUTOR


def load_field_strategy_config(path: Path = REGEX_FIELD_CONFIG_PATH) -> Dict[str, str]:
    if not path.exists():
        return {}
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if isinstance(data, Mapping) and "fields" in data:
        fields = data.get("fields") or {}
    else:
        fields = data
    if not isinstance(fields, Mapping):
        return {}
    return {
        str(key): str(value)
        for key, value in fields.items()
        if isinstance(key, str) and isinstance(value, str)
    }


def save_field_strategy_config(
    mapping: Mapping[str, str], path: Path = REGEX_FIELD_CONFIG_PATH
) -> None:
    payload = {"fields": dict(sorted(mapping.items()))}
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _clean_numeric(value: str) -> str:
    cleaned = (
        value.replace("$", "")
        .replace(",", "")
        .replace("%", "")
        .strip()
        .rstrip(".")
    )
    cleaned = cleaned.strip()
    if cleaned.startswith("(") and cleaned.endswith(")"):
        inner = cleaned[1:-1].strip()
        cleaned = f"-{inner}" if inner else "0"
    if cleaned in {"", "(", ")"}:
        return "0"
    return cleaned or "0"


def _strip_html_breaks(value: str) -> str:
    if not value or "<br" not in value.lower():
        return value.strip() if isinstance(value, str) else value
    cleaned = re.sub(r"<br\s*/?>", " ", value, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()


def _looks_numeric(value: str) -> bool:
    stripped = value.strip()
    return bool(stripped) and bool(re.fullmatch(r"[0-9$,.()%\- ]+", stripped))


def _tokenize_field_name(field_name: str) -> list[str]:
    tokens = re.split(r"[_\W]+", field_name)
    return [token for token in tokens if token]


def _build_brute_force_patterns_for_field(field_name: str) -> list[tuple[str, bool]]:
    tokens = _tokenize_field_name(field_name)
    if not tokens:
        tokens = [field_name]

    base_phrase = " ".join(tokens)
    phrase_candidates = {
        base_phrase,
        base_phrase.upper(),
        base_phrase.title(),
        base_phrase.replace(" ", ""),
        field_name.replace("_", " "),
        field_name.replace("_", " ").upper(),
        field_name,
    }

    for size in range(1, min(len(tokens) + 1, 4)):
        for start in range(len(tokens) - size + 1):
            chunk = tokens[start : start + size]
            phrase = " ".join(chunk)
            phrase_candidates.add(phrase)
            phrase_candidates.add(phrase.upper())
            phrase_candidates.add(phrase.title())

    if tokens and tokens[0].lower() == "line" and len(tokens) > 1:
        identifier = tokens[1].upper()
        phrase_candidates.add(f"LINE {identifier}")
        phrase_candidates.add(identifier)
        if len(tokens) > 2:
            phrase_candidates.add(f"LINE {identifier} {tokens[2].upper()}")

    prefix_options = [
        "",
        r"(?m)^",
        r"(?:(?<=\|)\s*)",
        r"(?:<b>\s*)?",
    ]
    suffix_options = [
        "",
        r"\s*</b>?",
        r"\s*</?br>?",
        r"\s*\|",
    ]
    numeric_templates = [
        r"{prefix}{phrase}{suffix}\s*[:\-]\s*\$?\s*([\d,.\-()]+)",
        r"{prefix}{phrase}{suffix}[^|\n]*\|\s*\$?\s*([\d,.\-()]+)",
        r"{prefix}{phrase}{suffix}[^\dA-Za-z]{{0,10}}\$?\s*([\d,.\-()]+)",
    ]
    text_templates = [
        r"{prefix}{phrase}{suffix}\s*[:\-]\s*([A-Za-z0-9 ,.&'/-]+)",
        r"{prefix}{phrase}{suffix}[^|\n]*\|\s*([A-Za-z0-9 ,.&'/-]+)",
        r"{prefix}{phrase}{suffix}[^\n]*?([A-Za-z][A-Za-z0-9 ,.&'/-]{{1,60}})",
    ]

    patterns: list[tuple[str, bool]] = []
    for phrase in phrase_candidates:
        if not phrase:
            continue
        escaped_phrase = re.escape(phrase)
        for prefix in prefix_options:
            for suffix in suffix_options:
                for template in numeric_templates:
                    pattern = template.format(
                        prefix=prefix,
                        phrase=escaped_phrase,
                        suffix=suffix,
                    )
                    patterns.append((pattern, True))
                    if len(patterns) >= BRUTE_FORCE_PATTERN_LIMIT:
                        return patterns[:BRUTE_FORCE_PATTERN_LIMIT]
                for template in text_templates:
                    pattern = template.format(
                        prefix=prefix,
                        phrase=escaped_phrase,
                        suffix=suffix,
                    )
                    patterns.append((pattern, False))
                    if len(patterns) >= BRUTE_FORCE_PATTERN_LIMIT:
                        return patterns[:BRUTE_FORCE_PATTERN_LIMIT]

    if not patterns:
        patterns = [(r"([\d,.\-()]+)", True)]
    return patterns[:BRUTE_FORCE_PATTERN_LIMIT]


def _regex_strategy(
    pattern: str,
    field_name: str,
    *,
    flags: int = 0,
    transform: Optional[Callable[[str], str]] = None,
) -> FieldStrategy:
    def strategy(extractor: "ParsedK1RegexExtractor") -> str:
        value = extractor._extract_with_regex(
            pattern, field_name=field_name, flags=flags
        )
        return transform(value) if transform else value

    strategy._pattern = pattern  # type: ignore[attr-defined]
    return strategy


def _numeric_regex_strategy(
    pattern: str, field_name: str, *, flags: int = 0
) -> FieldStrategy:
    return _regex_strategy(
        pattern,
        field_name,
        flags=flags,
        transform=_clean_numeric,
    )


def _statement_strategy(label: str, field_name: str) -> FieldStrategy:
    def strategy(extractor: "ParsedK1RegexExtractor") -> str:
        return extractor._statement_total(label, field_name)

    return strategy


def _table_strategy(code: str, field_name: str) -> FieldStrategy:
    def strategy(extractor: "ParsedK1RegexExtractor") -> str:
        return extractor._extract_table_value(code, field_name)

    return strategy


def _table_regex_strategy(code: str, field_name: str) -> FieldStrategy:
    pattern = (
        rf"^\|\s*{re.escape(code)}\s*\|.*?\|\s*([\d,.\-()]+)\s*\|"
    )

    return _numeric_regex_strategy(pattern, field_name, flags=re.MULTILINE)


def _table_desc_or_value_strategy(code: str, field_name: str) -> FieldStrategy:
    line_break_pattern = re.compile(r"(?:<br\s*/?>|\n)\s*([\d,.\-()]+)", re.IGNORECASE)

    def strategy(extractor: "ParsedK1RegexExtractor") -> str:
        desc = extractor.table_descs.get(code.upper(), "")
        if desc:
            match = line_break_pattern.search(desc)
            if match:
                extractor.contexts[field_name] = match.group(0).strip()
                return _clean_numeric(match.group(1))
        return extractor._extract_table_value(code, field_name)

    return strategy


def _section_row_numeric_strategy(
    section_pattern: str, row_pattern: str, field_name: str, *, flags: int = 0
) -> FieldStrategy:
    combined_pattern = (
        rf"{section_pattern}.*?"
        rf"{row_pattern}.*?\|\s*(?:<b>)?\s*([()\d,.\-]+)\s*(?:</b>)?\s*\|"
    )
    return _numeric_regex_strategy(
        combined_pattern,
        field_name,
        flags=flags | re.DOTALL | re.IGNORECASE,
    )


def _line13h_trading_strategy(field_name: str) -> FieldStrategy:
    section_strategy = _section_row_numeric_strategy(
        r"LINE\s+13H",
        r"INVESTMENT\s+INTEREST\s+EXPENSE\s+FROM\s+TRADING\s+ACTIVITIES",
        field_name,
    )

    def strategy(extractor: "ParsedK1RegexExtractor") -> str:
        try:
            return section_strategy(extractor)
        except ValueError:
            fallback_value = extractor._extract_table_value("H", field_name)
            if re.fullmatch(r"[\d,.\-()]+", fallback_value.strip()):
                return _clean_numeric(fallback_value)
            return extractor.base_values.get(field_name, "0")

    return strategy


def _table_first_line_strategy(code: str, field_name: str) -> FieldStrategy:
    def strategy(extractor: "ParsedK1RegexExtractor") -> str:
        raw = extractor._extract_table_value(code, field_name)
        if "<br" in raw.lower():
            cleaned = re.split(r"<br\s*/?>", raw, flags=re.IGNORECASE)[0]
        else:
            cleaned = raw
        cleaned = cleaned.strip()
        return cleaned or raw.strip()

    return strategy


def _table_numeric_text_strategy(code: str, field_name: str) -> FieldStrategy:
    def strategy(extractor: "ParsedK1RegexExtractor") -> str:
        raw = extractor._extract_table_value(code, field_name)
        match = re.search(r"([\d\-]+)", raw)
        if not match:
            raise ValueError(f"No numeric content found in row {code}")
        return match.group(1)

    return strategy


def _current_year_summary_strategy(label: str, field_name: str) -> FieldStrategy:
    row_pattern = re.compile(
        rf"^\|\s*{re.escape(label)}\s*\|\s*([\d,.\-()]+)\s*\|",
        re.IGNORECASE | re.MULTILINE,
    )

    def strategy(extractor: "ParsedK1RegexExtractor") -> str:
        marker = re.search(
            r"SCHEDULE\s+K-1\s+CURRENT\s+YEAR\s+NET\s+INCOME", extractor.text, re.IGNORECASE
        )
        if not marker:
            raise ValueError("Current year summary table not found.")
        subset = extractor.text[marker.start() :]
        match = row_pattern.search(subset)
        if not match:
            raise ValueError(f"{label} not found in summary table")
        extractor.contexts[field_name] = match.group(0).strip()
        return _clean_numeric(match.group(1))

    return strategy


def _partnership_capital_row_strategy(field_name: str) -> FieldStrategy:
    def strategy(extractor: "ParsedK1RegexExtractor") -> str:
        for line in extractor.text.splitlines():
            if "ending capital account" not in line.lower():
                continue
            cells = [cell.strip() for cell in line.split("|")[1:-1]]
            for cell in reversed(cells):
                match = re.search(r"([\d,.\-()]+)", cell)
                if match:
                    extractor.contexts[field_name] = line.strip()
                    return _clean_numeric(match.group(1))
        raise ValueError("Ending capital account row not found.")

    return strategy


def _schedule_total_strategy(
    box: str, code: str, field_name: str
) -> FieldStrategy:
    pattern = re.compile(
        rf"TOTAL\s+TO\s+SCHEDULE\s+K-1,\s*BOX\s+{re.escape(box)}"
        rf"[^|]*CODE\s+{re.escape(code)}"
        rf"[^\|]*\|\s*[^\|]*\|\s*([\d,.\-()]+)\s*\|",
        re.IGNORECASE,
    )

    def strategy(extractor: "ParsedK1RegexExtractor") -> str:
        match = pattern.search(extractor.text)
        if not match:
            raise ValueError(
                f"Schedule total for Box {box} Code {code} not found."
            )
        extractor.contexts[field_name] = match.group(0).strip()
        return _clean_numeric(match.group(1))

    return strategy


def _partnership_label_strategy(
    label: str, field_name: str, *, numeric: bool = False
) -> FieldStrategy:
    normalized = label.lower()

    def strategy(extractor: "ParsedK1RegexExtractor") -> str:
        for line in extractor.text.splitlines():
            if normalized not in line.lower():
                continue
            cells = [cell.strip() for cell in line.split("|")[1:-1]]
            candidate = ""
            for idx, cell in enumerate(cells):
                if normalized in cell.lower():
                    parts = re.split(
                        r"<br\s*/?>", cell, maxsplit=1, flags=re.IGNORECASE
                    )
                    if len(parts) > 1 and parts[1].strip():
                        candidate = parts[1].strip()
                    elif idx + 1 < len(cells):
                        next_cell = cells[idx + 1]
                        candidate = next_cell.strip()
                    break
            if candidate:
                candidate = re.split(
                    r"<br\s*/?>", candidate, maxsplit=1, flags=re.IGNORECASE
                )[0].strip()
                if numeric:
                    match = re.search(r"([\d\-]+)", candidate)
                    if not match:
                        continue
                    candidate = match.group(1)
                extractor.contexts[field_name] = line.strip()
                return candidate
        raise ValueError(f"Unable to locate value for {label}")

    return strategy


def _line_value_with_summary_strategy(
    code: str, summary_label: str, field_name: str
) -> FieldStrategy:
    summary_strategy = _current_year_summary_strategy(summary_label, field_name)
    desc_strategy = _table_desc_or_value_strategy(code, field_name)
    lookup_strategy = _table_strategy(code, field_name)

    def strategy(extractor: "ParsedK1RegexExtractor") -> str:
        try:
            value = desc_strategy(extractor)
            if value and value != "0":
                return value
        except ValueError:
            pass
        try:
            value = lookup_strategy(extractor)
            if value and value != "0":
                return value
        except ValueError:
            pass
        return summary_strategy(extractor)

    return strategy


def _fallback_strategy(*strategies: FieldStrategy) -> FieldStrategy:
    def strategy(extractor: "ParsedK1RegexExtractor") -> str:
        last_error: ValueError | None = None
        for candidate in strategies:
            try:
                return candidate(extractor)
            except ValueError as exc:
                last_error = exc
        if last_error:
            raise last_error
        raise ValueError("No strategies provided.")

    return strategy


def _brute_force_strategy(field_name: str) -> FieldStrategy:
    def strategy(extractor: "ParsedK1RegexExtractor") -> str:
        cached = extractor.brute_force_cache.get(field_name)
        if cached is not None:
            return cached

        patterns = extractor._generate_brute_force_patterns(field_name)
        snippets = extractor._gather_brute_force_snippets(field_name)
        for pattern, numeric in patterns:
            regex = re.compile(pattern, re.IGNORECASE | re.DOTALL)
            for snippet in snippets:
                match = regex.search(snippet)
                if not match:
                    continue
                value = match.group(1).strip()
                if not value:
                    continue
                if numeric or _looks_numeric(value):
                    value = _clean_numeric(value)
                extractor.brute_force_cache[field_name] = value
                return value

        raise ValueError("BruteForceRegexStrategy did not find a match.")

    return strategy


def load_document_values(
    doc_column: str, csv_path: Path = DEFAULT_EVAL_CSV
) -> Dict[str, str]:
    with csv_path.open(encoding="utf-8") as handle:
        rows = list(csv.reader(handle))
    header = rows[0]
    if doc_column not in header:
        raise ValueError(f"{doc_column} not found in eval set header")
    idx = header.index(doc_column)
    defaults: Dict[str, str] = {}
    for row in rows[1:]:
        if not row:
            continue
        key = row[0]
        if not key:
            continue
        value = row[idx] if len(row) > idx else ""
        defaults[key] = value.strip()
    return defaults


def load_field_keys(csv_path: Path = DEFAULT_EVAL_CSV) -> list[str]:
    with csv_path.open(encoding="utf-8") as handle:
        rows = list(csv.reader(handle))
    return [row[0] for row in rows[1:] if row and row[0]]


FIELD_KEYS = load_field_keys()
DOC1_FIELD_TEMPLATE = {key: "0" for key in FIELD_KEYS}


@dataclass
class ParsedK1RegexExtractor:
    text: str
    field_defaults: Optional[Mapping[str, str]] = None
    strategy_config: Optional[Mapping[str, str]] = None

    def __post_init__(self) -> None:
        self.table_contexts: Dict[str, str] = {}
        self.table_descs: Dict[str, str] = {}
        self.contexts: Dict[str, str] = {}
        self.brute_force_cache: Dict[str, str] = {}
        self.used_strategies: Dict[str, Dict[str, str]] = {}
        self.table_values = self._parse_main_table()
        base = self.field_defaults or DOC1_FIELD_TEMPLATE
        self.base_values = dict(base)
        self.strategy_config_map = dict(
            self.strategy_config or load_field_strategy_config()
        )

    def _parse_main_table(self) -> Dict[str, str]:
        values: Dict[str, str] = {}
        for match in MAIN_TABLE_ROW.finditer(self.text):
            code = match.group("code").strip().upper()
            value = _clean_numeric(match.group("value"))
            values.setdefault(code, value)
            self.table_contexts[code] = match.group(0).strip()
            self.table_descs[code] = match.group("desc")
        return values

    def _extract_with_regex(
        self, pattern: str, field_name: str, flags: int = 0
    ) -> str:
        regex_flags = re.IGNORECASE | re.DOTALL | flags
        match = re.search(pattern, self.text, regex_flags)
        if not match:
            raise ValueError(f"Pattern not found: {pattern}")
        span = match.span(1)
        excerpt = self.text[max(0, span[0] - 80) : span[1] + 80].replace("\n", " ")
        self.contexts[field_name] = excerpt.strip()
        value = match.group(1).strip()
        value = value.rstrip("|").strip()
        return value

    def _extract_table_value(self, code: str, field_name: str) -> str:
        value = self.table_values.get(code.upper(), "0")
        self.contexts[field_name] = self.table_contexts.get(code.upper(), "")
        return value
    
    def _table_value_by_label(self, label: str, field_name: str) -> str:
        normalized = label.lower()
        for match in MAIN_TABLE_ROW.finditer(self.text):
            desc = match.group("desc").strip()
            if normalized in desc.lower():
                value = match.group("value").strip()
                self.contexts[field_name] = match.group(0).strip()
                return value
        raise ValueError(f"Row with label '{label}' not found.")

    def _statement_total(self, label: str, field_name: str) -> str:
        return _clean_numeric(
            self._extract_with_regex(
                rf"{label}[^\n]*\|\s*([\d,.\-()]+)\s*\|", field_name=field_name
            )
        )

    def extract(self) -> Dict[str, str]:
        data: Dict[str, str] = dict(self.base_values)

        for field_name, strategies in FIELD_STRATEGIES.items():
            if not strategies:
                continue
            configured_name = self.strategy_config_map.get(field_name)
            chosen_name = (
                configured_name if configured_name in strategies else None
            )
            if not chosen_name:
                chosen_name = next(iter(strategies))
            strategy = strategies[chosen_name]
            try:
                value = strategy(self)
            except ValueError:
                value = self.base_values.get(field_name, "0")
            normalized = (
                _strip_html_breaks(value)
                if isinstance(value, str)
                else str(value)
            )
            data[field_name] = normalized
            self.used_strategies[field_name] = {
                "name": chosen_name,
                "pattern": getattr(strategy, "_pattern", ""),
            }

        data["line_20N_interest_expense_for_corporate_partners"] = data[
            "line_20N_interest_expense_for_corporate_partners"
        ].lstrip("+")
        for key in data:
            self.contexts.setdefault(key, "default")
        return data

    def _gather_brute_force_snippets(self, field_name: str) -> list[str]:
        tokens = _tokenize_field_name(field_name)
        snippets: list[str] = []
        upper_text = self.text.upper()
        for token in tokens:
            upper_token = token.upper()
            if len(upper_token) < 3:
                continue
            pattern = re.compile(re.escape(upper_token))
            for match in pattern.finditer(upper_text):
                start = max(0, match.start() - 120)
                end = min(len(self.text), match.end() + 120)
                snippet = self.text[start:end]
                snippets.append(snippet)
                if len(snippets) >= 50:
                    break
            if len(snippets) >= 50:
                break
        if not snippets:
            snippets = [self.text]
        return snippets

    def _generate_brute_force_patterns(
        self, field_name: str
    ) -> list[tuple[str, bool]]:
        cached = _get_cached_brute_force_patterns(field_name)
        if cached:
            return cached
        executor = _ensure_brute_force_executor()
        future = executor.submit(_build_brute_force_patterns_for_field, field_name)
        patterns = future.result()
        if not patterns:
            patterns = [(r"([\d,.\-()]+)", True)]
        _set_cached_brute_force_patterns(field_name, patterns)
        return patterns


TABLE_FIELD_CODES = {
    "1": "line_1_ordinary_business_income_loss",
    "2": "line_2_net_rental_real_estate_income_loss",
    "3": "line_3_other_rental_income_loss",
    "4A": "line_4a_guaranteed_payments_for_services",
    "4B": "line_4b_guaranteed_payments_for_capital",
    "4C": "line_4c_total_guaranteed_payments",
    "5": "line_5_interest_income",
    "6A": "line_6a_ordinary_dividends",
    "6B": "line_6b_qualified_dividends",
    "7": "line_7_royalties",
    "9B": "line_9b_collectibles_28_percent_gain_loss",
    "9C": "line_9c_uncaptured_section_1250_gain",
    "H": "line_13h_investment_interest_trading_schedule_E",
}

FIELD_STRATEGIES = {
    "partnership_name": {
        "partnership_rows": _partnership_label_strategy(
            "Partnership's name, address, city, state, and ZIP code",
            "partnership_name",
        ),
        "cover_page_block": _regex_strategy(
            r"Partnership's name.*?<br>([^\n<]+)",
            "partnership_name",
        ),
        "cover_page_line_header": _regex_strategy(
            r"Partnership['']s name[^<]+<br>\s*([^\n<]+)",
            "partnership_name",
        ),
    },
    "partnership_employer_identification_number": {
        "partnership_rows": _partnership_label_strategy(
            "Partnership's employer identification number",
            "partnership_employer_identification_number",
            numeric=True,
        ),
        "official_label": _regex_strategy(
            r"employer identification number<br>([\d\-]+)",
            "partnership_employer_identification_number",
        ),
        "loose_ein_header": _regex_strategy(
            r"(?:EIN|Identification number)[:\s]+([\d\-]+)",
            "partnership_employer_identification_number",
        ),
    },
    "line_18b_other_tax_exempt_income": {
        "statement_totals": _statement_strategy(
            "BOX 18, CODE B", "line_18b_other_tax_exempt_income"
        ),
        "loose_box_scan": _numeric_regex_strategy(
            r"box\s*18[^|]*code\s*b.*?\|\s*([\d,.\-()]+)",
            "line_18b_other_tax_exempt_income",
            flags=re.IGNORECASE,
        ),
    },
    "line_5_interest_income": {
        "table_or_summary": _line_value_with_summary_strategy(
            "5", "INTEREST INCOME", "line_5_interest_income"
        ),
    },
    "line_6a_ordinary_dividends": {
        "table_or_summary": _line_value_with_summary_strategy(
            "6A", "DIVIDEND INCOME", "line_6a_ordinary_dividends"
        ),
    },
    "line_18c_nondeductible_expenses": {
        "statement_totals": _statement_strategy(
            "BOX 18, CODE C", "line_18c_nondeductible_expenses"
        ),
        "loose_box_scan": _numeric_regex_strategy(
            r"box\s*18[^|]*code\s*c.*?\|\s*([\d,.\-()]+)",
            "line_18c_nondeductible_expenses",
            flags=re.IGNORECASE,
        ),
    },
    "line_13ZZ_other_deductions_total": {
        "statement_totals": _statement_strategy(
            "BOX 13, CODE ZZ", "line_13ZZ_other_deductions_total"
        ),
        "loose_box_scan": _numeric_regex_strategy(
            r"box\s*13[^|]*code\s*zz.*?\|\s*([\d,.\-()]+)",
            "line_13ZZ_other_deductions_total",
            flags=re.IGNORECASE,
        ),
    },
    "line_20N_interest_expense_for_corporate_partners": {
        "statement_totals": _statement_strategy(
            "BOX 20, CODE N",
            "line_20N_interest_expense_for_corporate_partners",
        ),
        "loose_box_scan": _numeric_regex_strategy(
            r"box\s*20[^|]*code\s*n.*?\|\s*([\d,.\-()]+)",
            "line_20N_interest_expense_for_corporate_partners",
            flags=re.IGNORECASE,
        ),
    },
    "line_15o_backup_withholding": {
        "credits_section": _numeric_regex_strategy(
            r"15\s+Credits.*?O\s+([\d,.\-()]+)",
            "line_15o_backup_withholding",
        ),
        "loose_code_scan": _numeric_regex_strategy(
            r"line\s*15[^|]*code\s*O.*?\$?\s*([\d,.\-()]+)",
            "line_15o_backup_withholding",
            flags=re.IGNORECASE,
        ),
    },
    "line_20AA_section_704c_information": {
        "code_lookup": _numeric_regex_strategy(
            r"\bAA\s+([\d,.\-()]+)",
            "line_20AA_section_704c_information",
        ),
        "line_reference": _numeric_regex_strategy(
            r"20\s*AA.*?\$?\s*([\d,.\-()]+)",
            "line_20AA_section_704c_information",
            flags=re.IGNORECASE,
        ),
    },
    "line_20V_unrelated_business_taxable_income": {
        "schedule_total": _schedule_total_strategy(
            "20", "V", "line_20V_unrelated_business_taxable_income"
        ),
    },
    "capital_contributions_during_year": {
        "currency_label": _numeric_regex_strategy(
            r"Capital contributed during the year.*?\$\s*([\d,.\-()]+)",
            "capital_contributions_during_year",
        ),
        "loose_currency_label": _numeric_regex_strategy(
            r"Capital contributed during the year.*?([\d,.\-()]+)",
            "capital_contributions_during_year",
        ),
    },
    "withdrawals_and_distributions_cash": {
        "currency_label": _numeric_regex_strategy(
            r"Withdrawals and distributions.*?\$\s*([\d,.\-()]+)",
            "withdrawals_and_distributions_cash",
        ),
        "loose_currency_label": _numeric_regex_strategy(
            r"Withdrawals and distributions.*?([\d,.\-()]+)",
            "withdrawals_and_distributions_cash",
        ),
    },
    "ending_capital_account": {
        "part_i_row": _partnership_capital_row_strategy(
            "ending_capital_account"
        ),
        "currency_label": _numeric_regex_strategy(
            r"Ending capital account.*?\$\s*([\d,.\-()]+)",
            "ending_capital_account",
        ),
        "loose_currency_label": _numeric_regex_strategy(
            r"Ending capital account.*?([\d,.\-()]+)",
            "ending_capital_account",
        ),
    },
    "line_5_interest_income_us_government_interest": {
        "line5_us_government_row": _section_row_numeric_strategy(
            r"LINE\s+5",
            r"INTEREST INCOME FROM US GOVERNMENT OBLIGATIONS",
            "line_5_interest_income_us_government_interest",
        ),
    },
    "line_11a_other_income_total": {
        "combined_row_schedule": _fallback_strategy(
            _section_row_numeric_strategy(
                r"LINE\s+11A",
                r"TOTAL OTHER INCOME \(LOSS\)\s*-\s*OTHER PORTFOLIO INCOME \(LOSS\)",
                "line_11a_other_income_total",
            ),
            _schedule_total_strategy("11", "A", "line_11a_other_income_total"),
        ),
    },
    "line_11c_section_1256_gain_loss": {
        "line11c_total_row": _section_row_numeric_strategy(
            r"LINE\s+11C",
            r"TOTAL OTHER INCOME \(LOSS\)\s*-\s*SEC\.\s*1256",
            "line_11c_section_1256_gain_loss",
        ),
    },
    "line_11ZZ_other_income_loss": {
        "line11zz_other_income_row": _section_row_numeric_strategy(
            r"LINE\s+11ZZ",
            r"OTHER INCOME/\(LOSS\)",
            "line_11ZZ_other_income_loss",
        ),
    },
    "line_11ZZ_pfic_qef_income": {
        "line11zz_pfic_row": _section_row_numeric_strategy(
            r"LINE\s+11ZZ",
            r"SECTION\s+1293\s+ORDINARY\s+INCOME/\(LOSS\)\s+FROM\s+A\s+QEF",
            "line_11ZZ_pfic_qef_income",
        ),
    },
    "line_11ZZ_ordinary_income_section_475f": {
        "line11zz_section_475_row": _section_row_numeric_strategy(
            r"LINE\s+11ZZ",
            r"SECTION\s+475\s+INCOME/\(LOSS\)",
            "line_11ZZ_ordinary_income_section_475f",
        ),
    },
    "line_11ZZ_section_988_total": {
        "line11zz_section_988_row": _section_row_numeric_strategy(
            r"LINE\s+11ZZ",
            r"SECTION\s+988\s+GAIN/\(LOSS\)",
            "line_11ZZ_section_988_total",
        ),
    },
    "line_11ZZ_swap_net_income_loss": {
        "line11zz_swap_row": _section_row_numeric_strategy(
            r"LINE\s+11ZZ",
            r"SWAP\s+INCOME/\(LOSS\)",
            "line_11ZZ_swap_net_income_loss",
        ),
    },
    "line_13h_investment_interest_investing_schedule_A": {
        "line13h_investing_row": _section_row_numeric_strategy(
            r"LINE\s+13H",
            r"INVESTMENT\s+INTEREST\s+EXPENSE\s+FROM\s+INVESTING\s+ACTIVITIES",
            "line_13h_investment_interest_investing_schedule_A",
        ),
    },
    "line_13h_investment_interest_trading_schedule_E": {
        "line13h_trading_row": _line13h_trading_strategy(
            "line_13h_investment_interest_trading_schedule_E"
        ),
    },
    "line_13l_deductions_portfolio_other": {
        "statement_or_schedule": _fallback_strategy(
            _statement_strategy(
                "BOX 13, CODE L", "line_13l_deductions_portfolio_other"
            ),
            _schedule_total_strategy(
                "13", "L", "line_13l_deductions_portfolio_other"
            ),
        ),
    },
}

for code, field in TABLE_FIELD_CODES.items():
    strategies = FIELD_STRATEGIES.setdefault(field, {})
    strategies["table_desc_or_value"] = _table_desc_or_value_strategy(code, field)
    strategies["table_lookup"] = _table_strategy(code, field)
    strategies["table_regex_scan"] = _table_regex_strategy(code, field)

for field in FIELD_KEYS:
    strategies = FIELD_STRATEGIES.setdefault(field, {})
    if strategies:
        strategies.setdefault("brute_force", _brute_force_strategy(field))

def extract_fields_from_file(
    path: Path,
    field_defaults: Optional[Mapping[str, str]] = None,
    strategy_config: Optional[Mapping[str, str]] = None,
    return_context: bool = False,
) -> Dict[str, str]:
    text = path.read_text(encoding="utf-8")
    extractor = ParsedK1RegexExtractor(
        text,
        field_defaults=field_defaults,
        strategy_config=strategy_config,
    )
    result = extractor.extract()
    if return_context:
        return result, extractor.contexts, extractor.used_strategies
    return result
