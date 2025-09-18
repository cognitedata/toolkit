import threading
from collections import UserDict
from collections.abc import Iterator
from typing import Any, Generic

from cognite_toolkit._cdf_tk.utils.useful_types import T_ID, T_Value


class ThreadSafeDict(UserDict, Generic[T_ID, T_Value]):
    """A thread-safe dictionary implementation.

    This class extends `UserDict` to provide a dictionary that is safe for concurrent access
    by multiple threads. It uses a threading lock to ensure that operations on the dictionary
    are atomic and do not lead to race conditions.

    It only supports one-thread reading/writing at a time, meaning that if one thread is reading,
    no other thread can read or write until the first thread is done. Todo: Support multiple readers
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._lock = threading.RLock()  # Initialize lock first
        super().__init__(*args, **kwargs)

    def __getitem__(self, key: T_ID) -> T_Value:
        with self._lock:
            return super().__getitem__(key)

    def __setitem__(self, key: T_ID, value: T_Value) -> None:
        with self._lock:
            super().__setitem__(key, value)

    def __delitem__(self, key: T_ID) -> None:
        with self._lock:
            super().__delitem__(key)

    def __len__(self) -> int:
        with self._lock:
            return super().__len__()

    def __contains__(self, key: object) -> bool:
        with self._lock:
            return super().__contains__(key)

    def __iter__(self) -> Iterator[T_ID]:
        with self._lock:
            # Create a copy of keys to avoid iteration issues
            return iter(list(self.data.keys()))

    def clear(self) -> None:
        with self._lock:
            super().clear()

    def copy(self) -> "ThreadSafeDict[T_ID, T_Value]":
        with self._lock:
            new_dict = ThreadSafeDict[T_ID, T_Value]()
            new_dict.data = self.data.copy()
            return new_dict

    def get(self, key: T_ID, default: T_Value | None = None) -> T_Value | None:  # type: ignore[override]
        with self._lock:
            return super().get(key, default)

    def pop(self, key: T_ID, *args: Any) -> T_Value:
        with self._lock:
            return super().pop(key, *args)

    def popitem(self) -> tuple[T_ID, T_Value]:
        with self._lock:
            return super().popitem()

    def setdefault(self, key: T_ID, default: T_Value | None = None) -> T_Value | None:
        with self._lock:
            return super().setdefault(key, default)

    def update(self, *args: Any, **kwargs: Any) -> None:
        with self._lock:
            super().update(*args, **kwargs)

    # For these we override to get a thread-safe operation.
    # keys, values, items in the super class returns views which are not thread-safe
    def keys(self) -> list[T_ID]:  # type: ignore[override]
        with self._lock:
            return list(self.data.keys())

    def values(self) -> list[T_Value]:  # type: ignore[override]
        with self._lock:
            return list(self.data.values())

    def items(self) -> list[tuple[T_ID, T_Value]]:  # type: ignore[override]
        with self._lock:
            return list(self.data.items())

    def __repr__(self) -> str:
        with self._lock:
            return f"{self.__class__.__name__}({dict(self.data)})"

    def __str__(self) -> str:
        with self._lock:
            return str(dict(self.data))
