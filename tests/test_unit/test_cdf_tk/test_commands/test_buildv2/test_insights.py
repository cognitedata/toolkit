import csv
import io
import json

from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import (
    ConsistencyError,
    InsightList,
    Recommendation,
)
from cognite_toolkit._cdf_tk.utils.file import format_insight_source_file


def test_insight_list_to_csv_preserves_multiline_message_and_fix(valid_yaml_absolute_path) -> None:
    """Multiline and special characters round-trip via the csv module; rows use LF only."""
    insights = InsightList(
        [
            ConsistencyError(
                message="summary line\nnext line",
                code="ERR-1",
                fix="do this\r\nthen that",
                source_files=[valid_yaml_absolute_path],
            ),
            Recommendation(
                message='text with "quotes" and, commas',
                code="REC-2",
                fix="single",
                source_files=[valid_yaml_absolute_path],
            ),
        ]
    )

    csv_text = insights.to_csv()
    assert "\r\n" not in csv_text, "record separators must be LF-only (unix CSV dialect)"
    rows = list(csv.DictReader(io.StringIO(csv_text), dialect=csv.unix_dialect))
    assert rows == [
        {
            "insight_type": "ConsistencyError",
            "code": "ERR-1",
            "source_file": format_insight_source_file(valid_yaml_absolute_path),
            "message": "summary line\nnext line",
            "fix": "do this\nthen that",
        },
        {
            "insight_type": "Recommendation",
            "code": "REC-2",
            "source_file": format_insight_source_file(valid_yaml_absolute_path),
            "message": 'text with "quotes" and, commas',
            "fix": "single",
        },
    ]


def test_insight_list_to_json_matches_structural_fields(valid_yaml_absolute_path) -> None:
    insights = InsightList(
        [
            ConsistencyError(message="a", code="C1", fix="f1", source_files=[valid_yaml_absolute_path]),
            Recommendation(message="b", code="B2", fix=None, source_files=[valid_yaml_absolute_path]),
        ]
    )
    parsed = json.loads(insights.to_json())
    assert parsed == [
        {
            "insightType": "ConsistencyError",
            "code": "C1",
            "sourceFile": format_insight_source_file(valid_yaml_absolute_path),
            "message": "a",
            "fix": "f1",
        },
        {
            "insightType": "Recommendation",
            "code": "B2",
            "sourceFile": format_insight_source_file(valid_yaml_absolute_path),
            "message": "b",
            "fix": None,
        },
    ]


def test_insight_list_from_csv_roundtrip(valid_yaml_absolute_path) -> None:
    original = InsightList(
        [
            ConsistencyError(
                message="summary line\nnext line",
                code="ERR-1",
                fix="do this",
                source_files=[valid_yaml_absolute_path],
            )
        ]
    )
    loaded = InsightList.from_csv(original.to_csv(), valid_yaml_absolute_path.parent)
    assert len(loaded) == 1
    assert loaded[0].message == original[0].message
    assert loaded[0].code == original[0].code
    assert loaded[0].fix == original[0].fix
    assert loaded[0].source_files == original[0].source_files


def test_insight_list_from_json_roundtrip(valid_yaml_absolute_path) -> None:
    original = InsightList(
        [
            Recommendation(message="b", code="B2", fix=None, source_files=[valid_yaml_absolute_path]),
        ]
    )
    loaded = InsightList.from_json(original.to_json(), valid_yaml_absolute_path.parent)
    assert len(loaded) == 1
    assert loaded[0].message == original[0].message
    assert loaded[0].code == original[0].code
    assert loaded[0].fix == original[0].fix
    assert loaded[0].source_files == original[0].source_files
