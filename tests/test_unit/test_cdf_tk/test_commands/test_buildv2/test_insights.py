import csv
import io

import pytest

from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import (
    BuildError,
    BuildWarning,
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
                source_file=valid_yaml_absolute_path,
            ),
            Recommendation(
                message='text with "quotes" and, commas',
                code="REC-2",
                fix="single",
                source_file=valid_yaml_absolute_path,
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

    def test_build_error_and_warning_roundtrip(self, valid_yaml_absolute_path) -> None:
        insights = InsightList(
            [
                BuildError(message="error", code="ERR-1", fix="fix", source_file=valid_yaml_absolute_path),
                BuildWarning(message="warning", code="WARN-1", fix="fix", source_file=valid_yaml_absolute_path),
            ]
        )
        organization_dir = valid_yaml_absolute_path.parent.parent

        assert InsightList.from_csv(insights.to_csv(), organization_dir).dump() == insights.dump()
        assert InsightList.from_json(insights.to_json(), organization_dir).dump() == insights.dump()

    def test_heading(self, valid_yaml_absolute_path) -> None:
        titled_insight = BuildError(
            message="m", code="INVALID-YAML", title="Invalid YAML", source_file=valid_yaml_absolute_path
        )
        untitled_insight = BuildError(message="m", code="SOME-CODE", source_file=valid_yaml_absolute_path)

        assert titled_insight.heading == "Invalid YAML"
        assert untitled_insight.heading == "Some code"

    def test_display_location_includes_position(self, valid_yaml_absolute_path) -> None:
        insight = BuildError(message="m", code="SOME-CODE", source_file=valid_yaml_absolute_path, line=3, column=6)

        assert insight.display_location == f"{insight.display_source_file_cwd}:3:6"

    def test_position_round_trips_through_csv(self, valid_yaml_absolute_path) -> None:
        insights = InsightList(
            [
                BuildError(message="m", code="A", source_file=valid_yaml_absolute_path, line=3, column=6),
                BuildError(message="m", code="B", source_file=valid_yaml_absolute_path),
            ]
        )
        organization_dir = valid_yaml_absolute_path.parent.parent

        loaded = InsightList.from_csv(insights.to_csv(), organization_dir)

        assert [(insight.line, insight.column) for insight in loaded] == [(3, 6), (None, None)]

    def test_insight_list_to_csv_preserves_multiline_message_and_fix(
        self, some_insights: InsightList, valid_yaml_absolute_path
    ) -> None:
        """Multiline and special characters round-trip via the csv module; rows use LF only."""
        csv_text = some_insights.to_csv()
        assert "\r\n" not in csv_text, "record separators must be LF-only (unix CSV dialect)"
        rows = list(csv.DictReader(io.StringIO(csv_text), dialect=csv.unix_dialect))
        assert rows == [
            {
                "insight_type": "ConsistencyError",
                "code": "ERR-1",
                "source_file": format_insight_source_file(valid_yaml_absolute_path),
                "line": "",
                "column": "",
                "message": "summary line\nnext line",
                "fix": "do this\nthen that",
            },
            {
                "insight_type": "Recommendation",
                "code": "REC-2",
                "source_file": format_insight_source_file(valid_yaml_absolute_path),
                "line": "",
                "column": "",
                "message": 'text with "quotes" and, commas',
                "fix": "single",
            },
        ]
