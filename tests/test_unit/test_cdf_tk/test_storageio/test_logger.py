from collections import Counter
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.storageio.logger import (
    FileDataLogger,
    FileWithAggregationLogger,
    ItemsResult,
    LabelResult,
    LogEntry,
    LogEntryV2,
    MemoryOperationTracker,
    NoOpLogger,
    NoOpTracker,
    Severity,
)
from cognite_toolkit._cdf_tk.utils.fileio import NDJsonWriter


@pytest.fixture
def tracker_with_issues() -> MemoryOperationTracker:
    """Create a tracker with pre-populated issues for testing."""
    tracker = MemoryOperationTracker()
    tracker.add_issue("item1", "issue A")
    tracker.add_issue("item1", "issue B")
    tracker.add_issue("item2", "issue A")
    return tracker


class TestMemoryOperationTracker:
    def test_track_items_and_finalize(self, tracker_with_issues: MemoryOperationTracker) -> None:
        tracker_with_issues.finalize_item("item1", "failure")
        tracker_with_issues.finalize_item("item2", "success")
        tracker_with_issues.finalize_item("item3", "unchanged")

        assert tracker_with_issues.get_status_counts() == {"failure": 1, "success": 1, "unchanged": 1}

    def test_get_issue_counts(self, tracker_with_issues: MemoryOperationTracker) -> None:
        tracker_with_issues.finalize_item("item1", "failure")
        tracker_with_issues.finalize_item("item2", "failure")

        assert tracker_with_issues.get_issue_counts("failure") == {"issue A": 2, "issue B": 1}

    def test_get_issue_counts_filtered_by_status(self, tracker_with_issues: MemoryOperationTracker) -> None:
        tracker_with_issues.finalize_item("item1", "failure")
        tracker_with_issues.finalize_item("item2", "success")

        assert tracker_with_issues.get_issue_counts(status="success") == {"issue A": 1}

    def test_finalize_item_with_list(self) -> None:
        tracker = MemoryOperationTracker()

        tracker.finalize_item(["a", "b", "c"], "success")

        assert tracker.get_status_counts() == {"success": 3}


class TestNoOpTracker:
    def test_all_operations_are_noop(self) -> None:
        tracker = NoOpTracker()

        tracker.add_issue("item1", "issue")
        tracker.finalize_item("item1", "success")
        tracker.finalize_item(["a", "b"], "failure")

        assert tracker.get_status_counts() == {}
        assert tracker.get_issue_counts(status="success") == {}


class TestNoOpLogger:
    def test_log_discards_entries(self) -> None:
        logger = NoOpLogger()

        logger.log(LogEntry(id="1", message="test"))
        logger.log([LogEntry(id="2", message="test2"), LogEntry(id="3", message="test3")])

        assert logger.tracker.get_status_counts() == {}


class TestFileDataLogger:
    def test_log_writes_to_writer(self) -> None:
        mock_write = MagicMock(spec=NDJsonWriter)
        logger = FileDataLogger(mock_write)

        logger.log(LogEntry(id="1"))
        logger.log([LogEntry(id="2"), LogEntry(id="3")])

        assert mock_write.write_chunks.call_count == 2
        actual = [call.args[0] for call in mock_write.write_chunks.call_args_list]
        assert actual == [
            [{"id": "1"}],
            [{"id": "2"}, {"id": "3"}],
        ]
        assert isinstance(logger.tracker, MemoryOperationTracker)


class TestFileWithAggregationLogger:
    def test_finalize(self) -> None:
        with FileWithAggregationLogger(MagicMock(spec=NDJsonWriter)) as logger:
            self._simulate_log_entries(logger)

            results = logger.finalize(is_dry_run=False)

        assert results == [
            ItemsResult(status="success", count=1),
            ItemsResult(status="failure", count=1, labels=[LabelResult("Could not write", count=1)]),
            ItemsResult(
                status="success-with-warning",
                count=2,
                labels=[
                    LabelResult(
                        "ignored values",
                        count=2,
                        attribute_name="ignored properties",
                        attribute_counter=Counter(["attribute", "attribute37", "attribute37"]),
                    )
                ],
            ),
        ]

    def _simulate_log_entries(self, logger: FileWithAggregationLogger) -> None:
        logger.register(["item_success", "item_failure", "item_warning1", "item_warning2"])

        logger.log(
            LogEntryV2(id="item_failure", severity=Severity.warning, label="ignored values", message="Will be ignored")
        )
        logger.log(
            LogEntryV2(id="item_failure", severity=Severity.failure, label="Could not write", message="Will be kept.")
        )

        logger.log(
            LogEntryV2(
                id="item_warning1",
                severity=Severity.warning,
                label="ignored values",
                message="Will be kept as there is no failure",
                attributes={"attribute", "attribute37"},
                attribute_display_name="ignored properties",
            )
        )
        logger.log(
            LogEntryV2(
                id="item_warning2",
                severity=Severity.warning,
                label="ignored values",
                message="Will be kept as there is no failure",
                attributes={"attribute37"},
                attribute_display_name="ignored properties",
            )
        )
