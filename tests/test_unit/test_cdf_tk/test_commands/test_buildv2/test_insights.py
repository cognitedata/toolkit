import csv
import io

import pytest

from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import (
    ConsistencyError,
    InsightList,
    Recommendation,
)
from cognite_toolkit._cdf_tk.utils.file import format_insight_source_file


@pytest.fixture
def some_insights(valid_yaml_absolute_path) -> InsightList:
    return InsightList(
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


class TestInsightList:
    def test_csv_roundtrip(self, some_insights) -> None:
        csv_text = some_insights.to_csv()
        loaded = InsightList.from_csv(csv_text, some_insights[0].source_file.parent.parent)

        assert some_insights.dump() == loaded.dump()

    def test_json_roundtrip(self, some_insights) -> None:
        json_text = some_insights.to_json()
        loaded = InsightList.from_json(json_text, some_insights[0].source_file.parent.parent)

        assert some_insights.dump() == loaded.dump()

    def test_insight_list_to_csv_preserves_multiline_message_and_fix(
        self, some_insights: InsightList, valid_yaml_absolute_path
    ) -> None:
        """Multiline and special characters round-trip via the csv module; rows use LF only."""
        csv_text = some_insights.to_csv()
        assert "\r\n" not in csv_text, "record separators must be LF-only (unix CSV dialect)"
        rows = list(csv.DictReader(io.StringIO(csv_text), dialect=csv.unix_dialect))
        assert rows == [
            {
                "alpha": "False",
                "insight_type": "ConsistencyError",
                "code": "ERR-1",
                "source_files": format_insight_source_file(valid_yaml_absolute_path),
                "message": "summary line\nnext line",
                "fix": "do this\nthen that",
            },
            {
                "alpha": "False",
                "insight_type": "Recommendation",
                "code": "REC-2",
                "source_files": format_insight_source_file(valid_yaml_absolute_path),
                "message": 'text with "quotes" and, commas',
                "fix": "single",
            },
        ]
