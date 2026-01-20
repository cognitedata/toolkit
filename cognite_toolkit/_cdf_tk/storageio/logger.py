from collections import defaultdict
from threading import Lock
from typing import Literal, TypeAlias

from pydantic import BaseModel
from pydantic.alias_generators import to_camel


class DataIssue(BaseModel, alias_generator=to_camel, extra="ignore", populate_by_name=True):
    """Represents an issue encountered during migration."""

    ...


OperationStatus: TypeAlias = Literal["success", "failure", "unchanged"]


class ItemTracker:
    """Tracks subcategories accumulated for a single item during pipeline processing."""

    def __init__(self, item_id: str) -> None:
        self.item_id = item_id
        self.subcategories: list[str] = []

    def add_subcategory(self, subcategory: str) -> None:
        """Add a subcategory/flag encountered during processing."""
        self.subcategories.append(subcategory)


class DataLogger:
    """Logger used for logging data operations such as the data plugin and migration commands.

    Thread-safe aggregation logger that tracks items through a pipeline,
    accumulating subcategories and finalizing with a status.
    """

    def __init__(self) -> None:
        self._lock = Lock()
        self._active_items: dict[str, ItemTracker] = {}
        self._status_counts: dict[OperationStatus, int] = defaultdict(int)
        self._subcategory_counts: dict[OperationStatus, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    def start_item(self, item_id: str) -> ItemTracker:
        """Start tracking an item entering the pipeline.

        Args:
            item_id: Unique identifier for the item.

        Returns:
            ItemTracker to accumulate subcategories during processing.
        """
        with self._lock:
            tracker = ItemTracker(item_id)
            self._active_items[item_id] = tracker
            return tracker

    def add_subcategory(self, item_id: str, subcategory: str) -> None:
        """Add a subcategory/flag to an item mid-pipeline.

        Args:
            item_id: The item's identifier.
            subcategory: The subcategory to add (e.g., "missing_metadata", "conversion_warning").
        """
        with self._lock:
            if item_id in self._active_items:
                self._active_items[item_id].add_subcategory(subcategory)

    def finalize_item(self, item_id: str, status: OperationStatus) -> None:
        """Finalize an item with its final status.

        Args:
            item_id: The item's identifier.
            status: Final status (success, failure, unchanged).
        """
        with self._lock:
            tracker = self._active_items.pop(item_id, None)
            self._status_counts[status] += 1
            if tracker is not None:
                for subcategory in tracker.subcategories:
                    self._subcategory_counts[status][subcategory] += 1

    def log_success(self, item_id: str) -> None:
        """Finalize item as successful."""
        self.finalize_item(item_id, "success")

    def log_failure(self, item_id: str) -> None:
        """Finalize item as failed."""
        self.finalize_item(item_id, "failure")

    def log_unchanged(self, item_id: str) -> None:
        """Finalize item as unchanged."""
        self.finalize_item(item_id, "unchanged")

    def get_status_counts(self) -> dict[OperationStatus, int]:
        """Get counts per final status."""
        with self._lock:
            return dict(self._status_counts)

    def get_subcategory_counts(self, status: OperationStatus | None = None) -> dict[OperationStatus, dict[str, int]]:
        """Get subcategory counts, optionally filtered by status."""
        with self._lock:
            if status is not None:
                return {status: dict(self._subcategory_counts[status])}
            return {s: dict(subs) for s, subs in self._subcategory_counts.items()}

    def reset(self) -> None:
        """Reset all counts and active items."""
        with self._lock:
            self._active_items.clear()
            self._status_counts.clear()
            self._subcategory_counts.clear()

    def log(self, issue: DataIssue) -> None:
        """Logs a data issue."""
        raise NotImplementedError()
