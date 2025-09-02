import threading
from collections.abc import Hashable
from typing import Generic, Literal, TypeAlias, TypeVar, get_args, overload

from cognite_toolkit._cdf_tk.utils import humanize_collection

Status: TypeAlias = Literal["pending", "failed", "success", "aborted"]

_ALLOWED_STATUS = get_args(Status)

T_ID = TypeVar("T_ID", bound=Hashable)


class ProgressTracker(Generic[T_ID]):
    def __init__(self, steps: list[str]) -> None:
        self._steps = list(steps)
        self._progress: dict[T_ID, dict[str, Status]] = {}
        self._lock = threading.Lock()

    def _init_item(self, item_id: T_ID) -> None:
        if item_id not in self._progress:
            self._progress[item_id] = {step: "pending" for step in self._steps}

    def set_progress(self, item_id: T_ID, step: str, status: Status) -> None:
        if status not in _ALLOWED_STATUS:
            raise ValueError(f"Status must be one of {humanize_collection(_ALLOWED_STATUS)}")
        with self._lock:
            self._init_item(item_id)
            try:
                idx = self._steps.index(step)
            except ValueError as e:
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
        with self._lock:
            self._init_item(item_id)
            if step is None:
                return self._progress[item_id]
            return self._progress[item_id][step]
