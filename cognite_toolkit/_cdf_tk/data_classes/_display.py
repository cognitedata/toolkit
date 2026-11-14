from dataclasses import dataclass

from rich.progress import ProgressBar


@dataclass
class CommandDisplay:
    """Class for controlling command display options"""

    intro: bool = True
    progress_bar: ProgressBar | None = None
    results: bool = True
