import queue
import threading
from collections.abc import Callable, Iterable, Sized
from typing import Any, Generic, TypeVar

from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    ProgressColumn,
    SpinnerColumn,
    Task,
    TaskID,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from cognite_toolkit._cdf_tk.exceptions import ToolkitRuntimeError

T_Download = TypeVar("T_Download", bound=Sized)
T_Processed = TypeVar("T_Processed", bound=Sized)


class ItemCountColumn(ProgressColumn):
    def render(self, task: Task) -> str:
        return f"[green]{int(task.fields.get('item_count', 0)):,} items"


class ProducerWorkerExecutor(Generic[T_Download, T_Processed]):
    """This class manages the execution of a producer-worker pattern with multiple threads.

    It downloads data from an iterable, processes it, and writes the processed data.

    Args:
        download_iterable (Iterable[T_Download]): An iterable that yields chunks of items.
        process (Callable[[T_Download], T_Processed]): A function that processes the chunks of items.
        write (Callable[[T_Processed], None]): A function that writes the processed items. This is typically to
            a file, CDF, or another storage format.
        iteration_count (int): The total number of iterations expected. Note this is not the total number of items,
            but rather the number of chunks of items that will be processed. Set to `None` if the total number of
            iterations is unknown.
        max_queue_size (int): The maximum size of the queues used for processing and writing. Note that the queue
            holds chunks of items, not individual items, so this should be set based on the expected size of the
            chunks being processed.
        download_description (str): A description of the download process, used for progress tracking.
        process_description (str): A description of the processing step, used for progress tracking.
        write_description (str): A description of the writing step, used for progress tracking.
        console (Console | None): An optional Rich Console instance for outputting progress and error messages.

    Examples:
        >>> from cognite_toolkit._cdf_tk.utils.producer_worker import ProducerWorkerExecutor
        >>> from typing import List
        >>> def download_data() -> Iterable[list[int]]:
        ...     yield [1, 2, 3, 4, 5]
        ...     yield [6, 7, 8, 9, 10]
        >>> def process_data(data: list[int]) -> list[int]:
        ...     return [x * 2 for x in data]
        >>> def write_data(data: list[int]) -> None:
        ...     print(f"Writing data: {data}")
        >>> executor = ProducerWorkerExecutor(
        ...     download_iterable=download_data(),
        ...     process=process_data,
        ...     write=write_data,
        ...     iteration_count=2,
        ...     max_queue_size=1,
        ... )
        >>> executor.run()

    """

    def __init__(
        self,
        download_iterable: Iterable[T_Download],
        process: Callable[[T_Download], T_Processed],
        write: Callable[[T_Processed], None],
        iteration_count: int | None,
        max_queue_size: int,
        download_description: str = "Producing",
        process_description: str = "Processing",
        write_description: str = "Writing",
        console: Console | None = None,
    ) -> None:
        self._download_iterable = download_iterable
        self._process = process
        self._write = write
        self.iteration_count = iteration_count
        self.download_description = download_description
        self.process_description = process_description
        self.write_description = write_description
        self.console = console or Console()
        self._stop_event = threading.Event()

        self.download_terminated = False
        self.is_processing = False

        # Queues for managing the flow of data between threads
        # Download -> [process_queue] -> Process -> [write_queue] -> Write
        self.process_queue: queue.Queue[T_Download] = queue.Queue(maxsize=max_queue_size)
        self.write_queue: queue.Queue[T_Processed] = queue.Queue(maxsize=max_queue_size)

        self.total_items = 0
        self.error_occurred = False
        self.error_message = ""

        self.stopped_by_user = False

    def _get_progress_columns(self) -> list[ProgressColumn]:
        """Helper to set up the progress bar and tasks based on iteration_count."""
        if self.iteration_count is None:
            return [
                SpinnerColumn(),
                TextColumn("[bold blue]{task.description}"),
                ItemCountColumn(),
                TimeElapsedColumn(),
            ]
        else:
            return [
                TextColumn("[bold blue]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                TimeRemainingColumn(),
            ]

    def run(self) -> None:
        def user_input_listener() -> None:
            input()
            self._stop_event.set()
            self.stopped_by_user = True
            self.console.print("[yellow]Execution stopped by user.[/yellow]")

        self.console.print(f"[blue]Starting {self.download_description} (Press enter to stop)...[/blue]")
        columns = self._get_progress_columns()
        with Progress(*columns, console=self.console) as progress:
            task_args: dict[str, Any] = (
                {"item_count": 0, "total": None} if self.iteration_count is None else {"total": self.iteration_count}
            )
            download_task = progress.add_task(self.download_description.title(), **task_args)
            process_task = progress.add_task(self.process_description.title(), **task_args)
            write_task = progress.add_task(self.write_description.title(), **task_args)

            download_thread = threading.Thread(target=self._download_worker, args=(progress, download_task))
            process_thread = threading.Thread(target=self._process_worker, args=(progress, process_task))
            write_thread = threading.Thread(target=self._write_worker, args=(progress, write_task))

            download_thread.start()
            process_thread.start()
            write_thread.start()

            input_thread = threading.Thread(target=user_input_listener, daemon=True)
            input_thread.start()

            for t in [download_thread, process_thread, write_thread]:
                try:
                    t.join()
                except KeyboardInterrupt:
                    self.console.print("[red]Execution interrupted by user.[/red]")
                    self.stopped_by_user = True
                    self._stop_event.set()
                    break

            self._stop_event.set()

    def raise_on_error(self) -> None:
        """Raises an exception if an error occurred during execution."""
        if self.error_occurred:
            raise ToolkitRuntimeError(f"An error occurred during execution: {self.error_message}")
        if self.stopped_by_user:
            raise ToolkitRuntimeError("Execution was stopped by the user.")

    def _download_worker(self, progress: Progress, download_task: TaskID) -> None:
        """Worker thread for downloading data."""
        try:
            iterator = iter(self._download_iterable)
        except TypeError as e:
            self.error_occurred = True
            self.error_message = str(e)
            self.console.print(f"[red]Error[/red] occurred while {self.download_description}: {self.error_message}")
            return
        item_count = 0
        while not self.error_occurred and not self._stop_event.is_set():
            try:
                items = next(iterator)
                self.total_items += len(items)
                item_count += len(items)
                while not self.error_occurred:
                    try:
                        self.process_queue.put(items, timeout=0.5)
                        progress.update(download_task, advance=1, item_count=item_count)
                        break  # Exit the loop once the item is successfully added
                    except queue.Full:
                        # Retry until the queue has space
                        continue
            except StopIteration:
                self.download_terminated = True
                break
            except Exception as e:
                self.error_occurred = True
                self.error_message = str(e)
                self.console.print(f"[red]Error[/red] occurred while {self.download_description}: {self.error_message}")
                break
        if self._stop_event.is_set():
            self.download_terminated = True

    def _process_worker(self, progress: Progress, process_task: TaskID) -> None:
        """Worker thread for processing data."""
        self.is_processing = True
        item_count = 0
        while (not self.download_terminated or not self.process_queue.empty()) and not self.error_occurred:
            try:
                items = self.process_queue.get(timeout=0.5)
                processed_items = self._process(items)
                self.write_queue.put(processed_items)
                item_count += len(items)
                progress.update(process_task, advance=1, item_count=item_count)
                self.process_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                self.error_occurred = True
                self.error_message = str(e)
                self.console.print(
                    f"[red]ErrorError[/red] occurred while {self.process_description}: {self.error_message}"
                )
                break
        self.is_processing = False

    def _write_worker(self, progress: Progress, write_task: TaskID) -> None:
        """Worker thread for writing data to file."""
        item_count = 0
        while (
            not self.download_terminated
            or self.is_processing
            or not self.write_queue.empty()
            or not self.process_queue.empty()
        ) and not self.error_occurred:
            try:
                items = self.write_queue.get(timeout=0.5)
                self._write(items)
                item_count += len(items)
                progress.update(write_task, advance=1, item_count=item_count)
                self.write_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                self.error_occurred = True
                self.error_message = str(e)
                self.console.print(f"[red]Error[/red] occurred while {self.write_description}: {self.error_message}")
                break
