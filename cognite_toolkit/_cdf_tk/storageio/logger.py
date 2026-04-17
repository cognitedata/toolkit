from abc import ABC, abstractmethod
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import Enum
from threading import Lock
from typing import Literal, TypeAlias

from pydantic import BaseModel, Field
from pydantic.alias_generators import to_camel
from rich.console import Console, Group
from rich.panel import Panel
from rich.tree import Tree

from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.fileio import NDJsonWriter


class Severity(Enum):
    warning = 1
    skipped = 2
    failure = 3


class LogAggregation(BaseModel, alias_generator=to_camel, extra="ignore", populate_by_name=True):
    """Storing the aggregated log entries"""

    id: str
    label: str
    severity: Severity
    attributes: set[str] | None = None
    attribute_display_name: str | None = None


class LogEntryV2(LogAggregation):
    message: str = Field(description="The details of the log entry.")

    def as_aggregation(self) -> LogAggregation:
        return LogAggregation.model_validate(self.model_dump(), extra="ignore")


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


class DataLogger(ABC):
    """Abstract base class for data loggers that track operations and log entries."""

    @abstractmethod
    def log(self, entry: LogEntryV2 | Sequence[LogEntryV2]) -> None:
        """Log a detailed entry."""
        raise NotImplementedError()

    def register(self, ids: list[str]) -> None: ...

    def apply_to_all_unprocessed(self, label: str, severity: Severity) -> None:
        """Sets the label and severity to all unprocessed items, i.e., all registered items without any log entry."""
        ...

    def force_write(self) -> None:
        """For loggers that batch log entries, this forces write immediately"""
        ...


class NoOpLogger(DataLogger):
    """A no-op logger that discards all log entries and does no tracking."""

    def log(self, entry: LogEntryV2 | Sequence[LogEntryV2]) -> None:
        """Discard the log entry (no-op)."""
        pass


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
    BATCH_SIZE: int = 1000
    NO_WARNINGS: int = 0

    def __init__(self, writer: NDJsonWriter) -> None:
        self._writer = writer
        self._lock = Lock()
        self._batch: list[LogEntryV2] = []
        self.aggregations_by_ids: dict[str, list[LogAggregation]] = {}

    def __enter__(self) -> "FileWithAggregationLogger":
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: object) -> None:
        self._write_to_file()
        return None

    def reset(self) -> None:
        """Reset all tracking data."""
        with self._lock:
            self.aggregations_by_ids.clear()

    def register(self, ids: list[str]) -> None:
        with self._lock:
            for id in ids:
                if id not in self.aggregations_by_ids:
                    self.aggregations_by_ids[id] = []
                else:
                    self._log_unlocked(
                        LogEntryV2(
                            id=id,
                            label="Toolkit bug - multiple registrations",
                            severity=Severity.warning,
                            message=f"Multiple registrations for {id}",
                        )
                    )

    def _update_aggregation_unlocked(self, entries: list[LogEntryV2]) -> None:
        """Internal method to update aggregations without acquiring the lock."""
        for entry in entries:
            if isinstance(entry, LogEntryV2):
                if entry.id in self.aggregations_by_ids:
                    self.aggregations_by_ids[entry.id].append(entry.as_aggregation())
                else:
                    self.aggregations_by_ids[entry.id] = [entry.as_aggregation()]
                    self._log_unlocked(
                        LogEntryV2(
                            id=entry.id,
                            label="Toolkit bug - missing registration",
                            severity=Severity.warning,
                            message=f"Missing registration for {entry.id}",
                        )
                    )

    def _log_unlocked(self, entry: LogEntryV2 | Sequence[LogEntryV2]) -> None:
        """Internal method to log entries without acquiring the lock."""
        entries = list(entry) if isinstance(entry, Sequence) else [entry]
        self._update_aggregation_unlocked(entries)
        self._batch.extend(entries)
        if len(self._batch) >= self.BATCH_SIZE:
            self._write_to_file_unlocked()

    def log(self, entry: LogEntryV2 | Sequence[LogEntryV2]) -> None:
        with self._lock:
            self._log_unlocked(entry)

    def _write_to_file_unlocked(self) -> None:
        """Internal method to write to file without acquiring the lock."""
        if self._batch:
            self._writer.write_chunks([e.model_dump(by_alias=True, mode="json") for e in self._batch])
            self._batch.clear()

    def _write_to_file(self) -> None:
        with self._lock:
            self._write_to_file_unlocked()

    def finalize(self, is_dry_run: bool) -> list[ItemsResult]:
        """Finalize logging and return aggregated results.

        Returns:
            List of ItemsResult grouped by status (derived from severity).
        """
        with self._lock:
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
        if max_severity == Severity.skipped.value:
            return "skipped"
        if max_severity == Severity.warning.value:
            return "pending-with-warning" if is_dry_run else "success-with-warning"
        return "pending" if is_dry_run else "success"

    def apply_to_all_unprocessed(self, label: str, severity: Severity) -> None:
        """Apply the given aggregation entry to all registered IDs that have no aggregations yet."""
        with self._lock:
            for id_, aggregations in self.aggregations_by_ids.items():
                if not aggregations:
                    aggregations.append(LogAggregation(id=id_, label=label, severity=severity))

    def force_write(self) -> None:
        self._write_to_file()


def display_item_results(items: list[ItemsResult], title: str, console: Console) -> None:
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

    trees: list[Tree] = []
    for item in sorted(items, key=lambda item: item.severity, reverse=True):
        style, icon = status_styles.get(item.status, ("white", "•"))
        tree = Tree(f"[{style}]{icon} {item.status}[/{style}]: {item.count} items")

        for label_result in item.labels:
            tree.add(f"[dim]{label_result.display_message()}[/dim]")

        trees.append(tree)

    console.print()
    console.print(Panel(Group(*trees), title=f"[bold]{title}[/bold]", expand=False))
