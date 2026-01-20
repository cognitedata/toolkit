from collections import defaultdict
from threading import Lock
from typing import Literal, TypeAlias

from pydantic import BaseModel
from pydantic.alias_generators import to_camel

from cognite_toolkit._cdf_tk.utils.fileio import NDJsonWriter


class LogEntry(BaseModel, alias_generator=to_camel, extra="ignore", populate_by_name=True):
    """Represents an issue encountered during migration."""

    ...


OperationStatus: TypeAlias = Literal["success", "failure", "unchanged", "pending"]


class ItemTracker:
    """Tracks issues accumulated for a single item during pipeline processing."""

    def __init__(self, item_id: str) -> None:
        self.item_id = item_id
        self.issues: list[str] = []

    def add_issue(self, issue: str) -> None:
        """Add an issue encountered during processing."""
        self.issues.append(issue)


class OperationTracker:
    """Tracks the overall operation progress and issues across multiple items.

    Tracks counts of final statuses and issues per status.
    """

    def __init__(self) -> None:
        self._lock = Lock()
        self._active_items: dict[str, ItemTracker] = {}
        self._status_counts: dict[OperationStatus, int] = defaultdict(int)
        self._issue_counts: dict[OperationStatus, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    def add_issue(self, item_id: str, issue: str) -> None:
        """Add an issue to an item, creating tracker if needed."""
        with self._lock:
            if item_id not in self._active_items:
                self._active_items[item_id] = ItemTracker(item_id)
            self._active_items[item_id].add_issue(issue)

    def finalize_item(self, item_id: str | list[str], status: OperationStatus) -> None:
        """Finalize an item with its final status.

        Args:
            item_id: The item's identifier.
            status: Final status (success, failure, unchanged).
        """
        with self._lock:
            if isinstance(item_id, list):
                for iid in item_id:
                    self._finalize_item_unlocked(iid, status)
            else:
                self._finalize_item_unlocked(item_id, status)

    def _finalize_item_unlocked(self, item_id: str, status: OperationStatus) -> None:
        """Internal method to finalize an item without acquiring the lock."""
        tracker = self._active_items.pop(item_id, None)
        self._status_counts[status] += 1
        if tracker is not None:
            for issue in tracker.issues:
                self._issue_counts[status][issue] += 1

    def get_status_counts(self) -> dict[OperationStatus, int]:
        """Get counts per final status."""
        with self._lock:
            return dict(self._status_counts)

    def get_issue_counts(self, status: OperationStatus | None = None) -> dict[OperationStatus, dict[str, int]]:
        """Get issue counts, optionally filtered by status."""
        with self._lock:
            if status is not None:
                return {status: dict(self._issue_counts[status])}
            return {s: dict(issues) for s, issues in self._issue_counts.items()}


class DataLogger:
    """Composes aggregation tracking with detailed file logging."""

    def __init__(self, writer: NDJsonWriter) -> None:
        self.tracker = OperationTracker()
        self._writer = writer

    def log(self, entry: LogEntry) -> None:
        """Log a detailed entry to the file."""
        self._writer.write_chunks([entry.model_dump(by_alias=True)])
