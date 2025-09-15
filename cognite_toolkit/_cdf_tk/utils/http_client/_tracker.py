import threading
from dataclasses import dataclass, field


@dataclass
class ItemsRequestTracker:
    """Tracks the state of requests split from an original request."""

    max_failures_before_abort: int = 0  # 0 means no early abort
    lock: threading.Lock = field(default_factory=threading.Lock, init=False)
    failed_split_count: int = field(default=0, init=False)
    successful_split_count: int = field(default=0, init=False)

    def register_failure(self) -> bool:
        """Register a failed split request and return whether to continue splitting."""
        with self.lock:
            self.failed_split_count += 1
            if self.max_failures_before_abort <= 0:
                return True  # Continue splitting (no limit)
            return self.failed_split_count < self.max_failures_before_abort

    def register_success(self) -> None:
        """Register a successful split request."""
        with self.lock:
            self.successful_split_count += 1
