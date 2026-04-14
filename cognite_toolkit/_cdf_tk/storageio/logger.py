from abc import ABC, abstractmethod
from collections import Counter, defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import Enum
from threading import Lock
from typing import Literal, TypeAlias

from pydantic import BaseModel, Field
from pydantic.alias_generators import to_camel
from rich.console import Console
from rich.tree import Tree

from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.fileio import NDJsonWriter


class LogEntry(BaseModel, alias_generator=to_camel, extra="ignore", populate_by_name=True):
    """Represents a log entry for tracking storage I/O operations."""

    id: str


class Severity(Enum):
    warning = 1
    failure = 2


class LogAggregation(LogEntry):
    """Storing the aggregated log entries"""

    label: str
    severity: Severity
    attributes: set[str] | None = None
    attribute_display_name: str | None = None


class LogEntryV2(LogAggregation):
    message: str = Field(description="The details of the log entry.")

    def as_aggregation(self) -> LogAggregation:
        return LogAggregation.model_validate(self.model_dump(), extra="ignore")


class LogIssue(LogEntry):
    message: str


OperationStatus: TypeAlias = Literal[
    "success", "failure", "pending", "skipped", "success-with-warning", "pending-with-warning"
]


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

    @abstractmethod
    def reset(self) -> None:
        """Reset all tracking data."""
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

    def reset(self) -> None:
        """No-op: Do nothing."""
        pass


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

    def reset(self) -> None:
        """Reset all tracking data."""
        with self._lock:
            self._active_items.clear()
            self._status_counts.clear()
            self._issue_counts.clear()


class DataLogger(ABC):
    """Abstract base class for data loggers that track operations and log entries."""

    tracker: OperationTracker

    @abstractmethod
    def log(self, entry: LogEntry | Sequence[LogEntry]) -> None:
        """Log a detailed entry."""
        raise NotImplementedError()

    def register(self, ids: list[str]) -> None: ...


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


@dataclass
class LabelResult:
    label: str
    count: int
    attribute_counter: Counter[str] = field(default_factory=Counter)
    attribute_name: str | None = None

    def display_message(self, most_common: int = 5) -> str:
        suffix = ""
        if self.attribute_counter and self.attribute_name:
            most_common_attributes = [attr for attr, _ in self.attribute_counter.most_common(most_common)]
            suffix = f" Most common {self.attribute_name}: {humanize_collection(most_common_attributes, sort=False)}."
        return f"{self.label}: {self.count} items.{suffix}"


@dataclass
class ItemsResult:
    status: OperationStatus
    count: int
    severity: int
    labels: list[LabelResult] = field(default_factory=list)

    def display_message(self) -> str:
        return f"{self.status}: {self.count} items."


class FileWithAggregationLogger(DataLogger):
    BATCH_SIZE = 1000
    NO_WARNINGS = 0

    def __init__(self, writer: NDJsonWriter) -> None:
        self.tracker = NoOpTracker()
        self._writer = writer
        self._batch: list[LogEntry] = []
        self.aggregations_by_ids: dict[str, list[LogAggregation]] = {}

    def __enter__(self) -> "FileWithAggregationLogger":
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: object) -> None:
        self._write_to_file()
        return None

    def register(self, ids: list[str]) -> None:
        for id in ids:
            if id not in self.aggregations_by_ids:
                self.aggregations_by_ids[id] = []
            else:
                self.log(
                    LogEntryV2(
                        id=id,
                        label="Toolkit bug - multiple registrations",
                        severity=Severity.warning,
                        message=f"Multiple registrations for {id}",
                    )
                )

    def _update_aggregation(self, entries: list[LogEntry]) -> None:
        for entry in entries:
            if isinstance(entry, LogEntryV2):
                if entry.id in self.aggregations_by_ids:
                    self.aggregations_by_ids[entry.id].append(entry)
                else:
                    self.aggregations_by_ids[entry.id] = [entry]
                    self.log(
                        LogEntryV2(
                            id=entry.id,
                            label="Toolkit bug - missing registration",
                            severity=Severity.warning,
                            message=f"Missing registration for {id}",
                        )
                    )

    def log(self, entry: LogEntry | Sequence[LogEntry]) -> None:
        entries = list(entry) if isinstance(entry, Sequence) else [entry]
        self._update_aggregation(entries)
        self._batch.extend(entries)
        if len(self._batch) >= self.BATCH_SIZE:
            self._write_to_file()

    def _write_to_file(self) -> None:
        if self._batch:
            self._writer.write_chunks([e.model_dump(by_alias=True) for e in self._batch])
            self._batch.clear()

    def finalize(self, is_dry_run: bool) -> list[ItemsResult]:
        """Finalize logging and return aggregated results.

        Flushes any remaining entries to file and aggregates logged entries
        by severity and label.

        Returns:
            List of ItemsResult grouped by status (derived from severity).
        """
        result_by_status: dict[OperationStatus, ItemsResult] = {}
        label_result_by_status_label: dict[OperationStatus, dict[str, LabelResult]] = {}
        for aggregations in self.aggregations_by_ids.values():
            max_severity = max((agg.severity.value for agg in aggregations), default=self.NO_WARNINGS)
            status = self._severity_to_status(max_severity, is_dry_run)
            if status not in result_by_status:
                result_by_status[status] = ItemsResult(status=status, count=0, severity=max_severity)
            result_by_status[status].count += 1
            if status not in label_result_by_status_label:
                label_result_by_status_label[status] = {}
            label_result_by_id = label_result_by_status_label[status]

            for aggregation in aggregations:
                if aggregation.severity.value != max_severity:
                    # We filter out all labels which are on a different severity level
                    # such that we only display the most severe issues for each item.
                    continue
                if aggregation.label not in label_result_by_id:
                    label_result_by_id[aggregation.label] = LabelResult(
                        label=aggregation.label,
                        count=0,
                        attribute_name=aggregation.attribute_display_name,
                    )
                label_result_by_id[aggregation.label].count += 1
                if aggregation.attributes:
                    label_result_by_id[aggregation.label].attribute_counter.update(aggregation.attributes)

        for result in result_by_status.values():
            if result.status in label_result_by_status_label:
                result.labels.extend(label_result_by_status_label[result.status].values())

        return list(result_by_status.values())

    def _severity_to_status(self, max_severity: int, is_dry_run: bool) -> OperationStatus:
        if max_severity == Severity.failure.value:
            return "failure"
        elif max_severity == Severity.warning.value:
            return "pending-with-warning" if is_dry_run else "success-with-warning"
        else:
            return "pending" if is_dry_run else "success"


def display_item_results(items: list[ItemsResult], console: Console) -> None:
    """Display item results using rich formatting.

    Shows a tree view of items grouped by status, with their labels and counts.
    """
    if not items:
        return

    status_styles: dict[OperationStatus, tuple[str, str]] = {
        "success": ("green", "✓"),
        "failure": ("red", "✗"),
        "pending": ("cyan", "○"),
        "skipped": ("dim", "⊘"),
        "success-with-warning": ("yellow", "⚠"),
        "pending-with-warning": ("yellow", "○"),
    }

    for item in sorted(items, key=lambda item: item.severity, reverse=True):
        style, icon = status_styles.get(item.status, ("white", "•"))
        tree = Tree(f"[{style}]{icon} {item.status}[/{style}]: {item.count} items")

        for label_result in item.labels:
            tree.add(f"[dim]{label_result.display_message()}[/dim]")

        console.print(tree)
