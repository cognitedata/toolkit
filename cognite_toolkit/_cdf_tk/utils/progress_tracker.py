import threading
from collections import Counter
from typing import Generic, Literal, TypeAlias, get_args, overload

from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.useful_types import T_ID

Status: TypeAlias = Literal["success", "failed", "aborted", "pending"]

AVAILABLE_STATUS = get_args(Status)


class ProgressTracker(Generic[T_ID]):
    """A thread-safe progress tracker for multiple items and steps.

    For example, when migrating asset-centric Assets to CogniteAsset, we would like to track the progress of each Asset
    through the steps of the migration. This is such that we can report back to the user which Assets failed, where
    they failed, or if they succeeded.

    Args:
        steps (list[str]): A list of unique step names to track progress for.

    Examples:
        >>> tracker = ProgressTracker(steps=["downloadAsset", "convert", "uploadCogniteAssset"])
        >>> tracker.set_progress(item_id=123, step="downloadAsset", status="success")
        >>> tracker.set_progress(item_id=123, step="convert", status="failed")
        >>> tracker.get_progress(item_id=123)
        {'downloadAsset': 'success', 'convert': 'failed', 'uploadCogniteAssset': 'aborted'}

    """

    def __init__(self, steps: list[str]) -> None:
        if len(steps) != len(set(steps)):
            raise ValueError("Input `steps` must be unique.")
        self._steps = list(steps)
        self._step_to_idx: dict[str, int] = {step: i for i, step in enumerate(self._steps)}
        self._progress: dict[T_ID, dict[str, Status]] = {}
        self._lock = threading.Lock()

    def _init_item(self, item_id: T_ID) -> None:
        if item_id not in self._progress:
            self._progress[item_id] = {step: "pending" for step in self._steps}

    def set_progress(self, item_id: T_ID, step: str, status: Status) -> None:
        """Set the progress of a specific step for a given item."""
        if status not in AVAILABLE_STATUS:
            raise ValueError(f"Status must be one of {humanize_collection(AVAILABLE_STATUS)}")
        with self._lock:
            self._init_item(item_id)
            try:
                idx = self._step_to_idx[step]
            except KeyError as e:
                raise ValueError(f"Step '{step}' not found in steps {humanize_collection(self._steps)}") from e
            self._progress[item_id][step] = status
            if status == "failed":
                for s in self._steps[idx + 1 :]:
                    self._progress[item_id][s] = "aborted"

    @overload
    def get_progress(self, item_id: T_ID) -> dict[str, Status]: ...

    @overload
    def get_progress(self, item_id: T_ID, step: str) -> Status: ...

    def get_progress(self, item_id: T_ID, step: str | None = None) -> dict[str, Status] | Status:
        """Get the progress of all steps or a specific step for a given item."""
        with self._lock:
            self._init_item(item_id)
            if step is None:
                return self._progress[item_id].copy()
            if step not in self._step_to_idx:
                raise ValueError(f"Step '{step}' not found in steps {humanize_collection(self._steps)}")
            return self._progress[item_id][step]

    def result(self) -> dict[T_ID, dict[str, Status]]:
        """Get the progress of all items."""
        with self._lock:
            return {item_id: progress.copy() for item_id, progress in self._progress.items()}

    def aggregate(self) -> dict[tuple[str, Status], int]:
        """Aggregate the progress across all items and steps.

        Returns:
            dict[tuple[str, Status], int]: A dictionary with keys as (step, status) tuples and values as counts.
        """
        with self._lock:
            return Counter((step, status) for progress in self._progress.values() for step, status in progress.items())
