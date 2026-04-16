from collections import Counter
from unittest.mock import MagicMock

from cognite_toolkit._cdf_tk.storageio.logger import (
    FileWithAggregationLogger,
    ItemsResult,
    LabelResult,
    LogEntryV2,
    Severity,
    display_item_results,
)
from cognite_toolkit._cdf_tk.utils.fileio import NDJsonWriter


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
