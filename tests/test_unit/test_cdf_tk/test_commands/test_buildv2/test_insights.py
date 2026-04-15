import csv
import io

from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import (
    ConsistencyError,
    InsightList,
    Recommendation,
)


def test_insight_list_to_csv_preserves_multiline_message_and_fix() -> None:
    """Multiline and special characters round-trip via the csv module; rows use LF only."""
    insights = InsightList(
        [
            ConsistencyError(
                message="summary line\nnext line",
                code="ERR-1",
                fix="do this\r\nthen that",
            ),
            Recommendation(
                message='text with "quotes" and, commas',
                code=None,
                fix="single",
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
            "message": "summary line\nnext line",
            "fix": "do this\nthen that",
        },
        {
            "insight_type": "Recommendation",
            "code": "",
            "message": 'text with "quotes" and, commas',
            "fix": "single",
        },
    ]
