from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass

from rich.console import Console
from rich.progress import Progress


@dataclass(frozen=True)
class CommandDisplay:
    """Class for controlling command display options"""

    header: bool = True
    progress: Progress | None = None
    results: bool = True

    @contextmanager
    def progress_bar(self, console: Console) -> Generator[Progress]:
        """Yields the shared progress if one was given, otherwise a local one that is started and stopped here."""
        if self.progress is not None:
            yield self.progress
        else:
            with Progress(console=console) as progress:
                yield progress
