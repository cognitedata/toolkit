from collections import Counter
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.dataio.logger import (
    FileWithAggregationLogger,
    ItemsResult,
    LabelResult,
    LogEntryV2,
    Severity,
    display_item_results,
)
from cognite_toolkit._cdf_tk.utils.fileio import NDJsonWriter


class FakeMonotonic:
    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value


class TestFileWithAggregationLogger:
    def test_finalize(self) -> None:
        with FileWithAggregationLogger(MagicMock(spec=NDJsonWriter)) as logger:
            self._simulate_log_entries(logger)

            results = logger.finalize(is_dry_run=False)

        assert results == [
            ItemsResult(status="success", count=1, severity=0),
            ItemsResult(
                status="failure",
                count=1,
                labels=[LabelResult("Could not write", count=1)],
                severity=Severity.failure.value,
            ),
            ItemsResult(
                status="success-with-warning",
                count=2,
                severity=Severity.warning.value,
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

        # Just to ensure that no exception is raised.
        display_item_results(results, "Title", MagicMock())

    def test_force_write_flushes_batch_below_size(self) -> None:
        writer = MagicMock(spec=NDJsonWriter)
        logger = FileWithAggregationLogger(writer)
        logger.register(["item1"])
        logger.log(LogEntryV2(id="item1", severity=Severity.warning, label="x", message="m"))

        writer.write_chunks.assert_not_called()

        logger.force_write()

        writer.write_chunks.assert_called_once()
        writer.flush.assert_called()

    def test_flushes_batch_after_interval(self, monkeypatch: pytest.MonkeyPatch) -> None:
        clock = FakeMonotonic()
        monkeypatch.setattr("cognite_toolkit._cdf_tk.dataio.logger.time.monotonic", clock)
        writer = MagicMock(spec=NDJsonWriter)
        logger = FileWithAggregationLogger(writer)
        logger.register(["item1", "item2"])
        logger.log(LogEntryV2(id="item1", severity=Severity.warning, label="x", message="m"))

        writer.write_chunks.assert_not_called()

        clock.value = FileWithAggregationLogger.FLUSH_INTERVAL_SECONDS
        logger.log(LogEntryV2(id="item2", severity=Severity.warning, label="x", message="m"))

        writer.write_chunks.assert_called_once()
        writer.flush.assert_called()

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
