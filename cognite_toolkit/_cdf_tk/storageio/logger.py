from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Sequence
from threading import Lock
from typing import Literal, TypeAlias

from pydantic import BaseModel
from pydantic.alias_generators import to_camel

from cognite_toolkit._cdf_tk.utils.fileio import NDJsonWriter


class LogEntry(BaseModel, alias_generator=to_camel, extra="ignore", populate_by_name=True):
    """Represents a log entry for tracking storage I/O operations."""

    id: str


OperationStatus: TypeAlias = Literal["success", "failure", "unchanged", "pending"]


class ItemTracker:
    """Tracks issues accumulated for a single item during pipeline processing."""

    def __init__(self, item_id: str) -> None:
        self.item_id = item_id
        self.issues: list[str] = []

    def add_issue(self, issue: str) -> None:
        """Add an issue encountered during processing."""
        self.issues.append(issue)


class OperationTracker(ABC):
    """Abstract base class for operation trackers."""

    @abstractmethod
    def add_issue(self, item_id: str, issue: str) -> None:
        """Add an issue to an item."""
        raise NotImplementedError()

    @abstractmethod
    def finalize_item(self, item_id: str | list[str], status: OperationStatus) -> None:
        """Finalize an item with its final status."""
        raise NotImplementedError()

    @abstractmethod
    def get_status_counts(self) -> dict[OperationStatus, int]:
        """Get counts per final status."""
        raise NotImplementedError()

    @abstractmethod
    def get_issue_counts(self, status: OperationStatus) -> dict[str, int]:
        """Get issue counts, optionally filtered by status."""
        raise NotImplementedError()


class NoOpTracker(OperationTracker):
    """A no-op tracker that does nothing."""

    def add_issue(self, item_id: str, issue: str) -> None:
        """No-op: Discard the issue."""
        pass

    def finalize_item(self, item_id: str | list[str], status: OperationStatus) -> None:
        """No-op: Do nothing."""
        pass

    def get_status_counts(self) -> dict[OperationStatus, int]:
        """Return empty status counts."""
        return {}

    def get_issue_counts(self, status: OperationStatus) -> dict[str, int]:
        """Return empty issue counts."""
        return {}


class MemoryOperationTracker(OperationTracker):
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

    def get_issue_counts(self, status: OperationStatus) -> dict[str, int]:
        """Get issue counts, optionally filtered by status."""
        with self._lock:
            return dict(self._issue_counts.get(status, {}))


class DataLogger(ABC):
    """Abstract base class for data loggers that track operations and log entries."""

    tracker: OperationTracker

    @abstractmethod
    def log(self, entry: LogEntry | Sequence[LogEntry]) -> None:
        """Log a detailed entry."""
        raise NotImplementedError()


class NoOpLogger(DataLogger):
    """A no-op logger that discards all log entries and does no tracking."""

    def __init__(self) -> None:
        self.tracker = NoOpTracker()

    def log(self, entry: LogEntry | Sequence[LogEntry]) -> None:
        """Discard the log entry (no-op)."""
        pass


class FileDataLogger(DataLogger):
    """Composes aggregation tracking with detailed file logging."""

    def __init__(self, writer: NDJsonWriter) -> None:
        self.tracker = MemoryOperationTracker()
        self._writer = writer

    def log(self, entry: LogEntry | Sequence[LogEntry]) -> None:
        """Log a detailed entry to the file."""
        entries = entry if isinstance(entry, Sequence) else [entry]
        self._writer.write_chunks([e.model_dump(by_alias=True) for e in entries])
