import threading
from dataclasses import dataclass, field


@dataclass
class ItemsRequestTracker:
    """Tracks the state of requests split from an original request.

    Attributes:
        max_failures_before_abort (int): Maximum number of allowed failed split requests before aborting
            the entire operation. A value of -1 indicates no early abort.
        lock (threading.Lock): A lock to ensure thread-safe updates to the failure count.
        failed_split_count (int): The current count of failed split requests.

    """

    max_failures_before_abort: int = -1  # -1 means no early abort
    lock: threading.Lock = field(default_factory=threading.Lock, init=False)
    failed_split_count: int = field(default=0, init=False)

    def register_failure(self) -> None:
        """Register a failed split request and return whether to continue splitting."""
        with self.lock:
            self.failed_split_count += 1

    def limit_reached(self) -> bool:
        """Check if the failure limit has been reached."""
        with self.lock:
            if self.max_failures_before_abort < 0:
                return False
            return self.failed_split_count >= self.max_failures_before_abort
