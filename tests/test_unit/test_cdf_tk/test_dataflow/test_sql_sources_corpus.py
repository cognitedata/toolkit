from collections import Counter
from dataclasses import asdict
from typing import Any

import pytest
import yaml

from cognite_toolkit._cdf_tk.utils.sql_sources import SqlSource, extract_sql_sources
from tests.constants import REPO_ROOT

pytest.importorskip("sqlglot")

CORPUS = REPO_ROOT / "tests/data/dataflow_corpus"
CASES = sorted(path.stem for path in CORPUS.glob("*.sql"))


def _as_label(source: SqlSource) -> dict[str, Any]:
    # confidence and reason are not part of the labels, they are derived from the kind and the parse.
    data = {key: value for key, value in asdict(source).items() if value is not None}
    for key in ("confidence", "reason"):
        data.pop(key, None)
    data["kind"] = source.kind.value
    return data


@pytest.mark.parametrize("case", CASES)
def test_corpus_case(case: str) -> None:
    expected = yaml.safe_load((CORPUS / f"{case}.expected.yaml").read_text(encoding="utf-8"))

    result = extract_sql_sources((CORPUS / f"{case}.sql").read_text(encoding="utf-8"))

    assert result.parse_status.value == expected["parse_status"]
    assert [_as_label(source) for source in result.sources] == expected["sources"]


def test_corpus_parse_rate_does_not_regress() -> None:
    baseline = yaml.safe_load((CORPUS / "baseline.yaml").read_text(encoding="utf-8"))

    statuses = Counter(
        extract_sql_sources((CORPUS / f"{case}.sql").read_text(encoding="utf-8")).parse_status.value for case in CASES
    )

    assert statuses["ok"] >= baseline["ok"], f"Parse status per query: {dict(statuses)}"
