import queue
import sys
import threading
import time
import typing
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
T_Item = TypeVar("T_Item", bound=Sized)

# Sentinels for signaling finish
PROCESS_FINISH_SENTINEL = object()
WRITE_FINISH_SENTINEL = object()


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
        self._error_event = threading.Event()
        # Queues for managing the flow of data between threads
        # Download -> [process_queue] -> Process -> [write_queue] -> Write
        self.process_queue: queue.Queue[T_Download] = queue.Queue(maxsize=max_queue_size)
        self.write_queue: queue.Queue[T_Processed] = queue.Queue(maxsize=max_queue_size)
        self.total_items = 0
        self.error_message = ""

    @property
    def error_occurred(self) -> bool:
        """Indicates if an error occurred during execution."""
        return self._error_event.is_set()

    @property
    def stopped_by_user(self) -> bool:
        """Indicates if the execution was stopped by the user."""
        return self._stop_event.is_set()

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

    def _user_input_listener(self, producer_thread: threading.Thread) -> None:
        while not self._error_event.is_set() and not self._stop_event.is_set():
            key = getch(timeout=0.1)
            if key is None and not producer_thread.is_alive():
                break
            elif key is None:
                continue
            elif key.casefold() == "q":
                self._stop_event.set()
                self.console.print(
                    f"[yellow]Execution stopped by user. Finishing: {self.write_description}...[/yellow]"
                )
                break

    def run(self) -> None:
        self.console.print(f"[blue]Starting {self.download_description} (Press 'q' to stop)...[/blue]")
        columns = self._get_progress_columns()
        with Progress(*columns, console=self.console) as progress:
            task_args: dict[str, Any] = (
                {"item_count": 0, "total": None} if self.iteration_count is None else {"total": self.iteration_count}
            )
            download_task = progress.add_task(self.download_description, **task_args)
            process_task = progress.add_task(self.process_description, **task_args)
            write_task = progress.add_task(self.write_description, **task_args)

            download_thread = threading.Thread(target=self._download_worker, args=(progress, download_task))
            process_thread = threading.Thread(target=self._process_worker, args=(progress, process_task))
            write_thread = threading.Thread(target=self._write_worker, args=(progress, write_task))

            download_thread.start()
            process_thread.start()
            write_thread.start()

            input_thread = threading.Thread(target=self._user_input_listener, args=(download_thread,))
            input_thread.start()

            for t in [download_thread, process_thread, write_thread]:
                try:
                    t.join()
                except KeyboardInterrupt:
                    self.console.print("[red]Execution interrupted by user.[/red]")
                    self._stop_event.set()
                    break

            # After a possible interrupt, we must wait for all threads to finish their
            # graceful shutdown. This is important to prevent data loss.
            for t in [download_thread, process_thread, write_thread, input_thread]:
                if t.is_alive():
                    t.join()

    def raise_on_error(self) -> None:
        """Raises an exception if an error occurred during execution."""
        if self._error_event.is_set():
            raise ToolkitRuntimeError(f"An error occurred during execution: {self.error_message}")
        if self._stop_event.is_set():
            raise ToolkitRuntimeError("Execution was stopped by the user.")

    def _download_worker(self, progress: Progress, download_task: TaskID) -> None:
        """Worker thread for downloading data."""
        try:
            iterator = iter(self._download_iterable)
        except TypeError as e:
            self._error_event.set()
            self.error_message = str(e)
            self.console.print(f"[red]Error[/red] occurred while {self.download_description}: {self.error_message}")
            return
        item_count = 0
        while not self._error_event.is_set():
            try:
                if self._stop_event.is_set():
                    break
                items = next(iterator)
                self.total_items += len(items)
                if self._put_with_error_check(items, self.process_queue):
                    item_count += len(items)
                    progress.update(download_task, advance=1, item_count=item_count)
                    continue
                break  # Exit if error event was set while waiting to put
            except StopIteration:
                break
            except Exception as e:
                self._error_event.set()
                self.error_message = str(e)
                self.console.print(f"[red]Error[/red] occurred while {self.download_description}: {self.error_message}")
                break
        self._put_with_error_check(PROCESS_FINISH_SENTINEL, self.process_queue)  # type: ignore[misc]

    def _put_with_error_check(self, items: T_Item, target_queue: queue.Queue[T_Item]) -> bool:
        """Helper to put items into a queue with error checking."""
        while not self._error_event.is_set():
            try:
                target_queue.put(items, timeout=0.5)
                return True
            except queue.Full:
                continue
        return False

    def _process_worker(self, progress: Progress, process_task: TaskID) -> None:
        """Worker thread for processing data."""
        item_count = 0
        while not self._error_event.is_set():
            try:
                items = self.process_queue.get(timeout=0.5)
                if items is PROCESS_FINISH_SENTINEL:
                    # Signal writer to finish
                    self._put_with_error_check(WRITE_FINISH_SENTINEL, self.write_queue)  # type: ignore[misc]
                    self.process_queue.task_done()
                    break
                processed_items = self._process(items)
                if self._put_with_error_check(processed_items, self.write_queue):
                    item_count += len(items)
                    progress.update(process_task, advance=1, item_count=item_count)
                    self.process_queue.task_done()
                    continue
                else:
                    self.process_queue.task_done()
                    break  # Exit if error event was set while waiting to put
            except queue.Empty:
                continue
            except Exception as e:
                self._error_event.set()
                self.error_message = str(e)
                self.console.print(f"[red]Error[/red] occurred while {self.process_description}: {self.error_message}")
                break

    def _write_worker(self, progress: Progress, write_task: TaskID) -> None:
        """Worker thread for writing data to file."""
        item_count = 0
        while not self._error_event.is_set():
            try:
                items = self.write_queue.get(timeout=0.5)
                if items is WRITE_FINISH_SENTINEL:
                    self.write_queue.task_done()
                    break
                self._write(items)
                item_count += len(items)
                progress.update(write_task, advance=1, item_count=item_count)
                self.write_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                self._error_event.set()
                self.error_message = str(e)
                self.console.print(f"[red]Error[/red] occurred while {self.write_description}: {self.error_message}")
                break


# MyPy fails as the imports are os specific
# thus we disable type checking for this function
@typing.no_type_check
def getch(timeout: float) -> str | None:
    """Get a single character from standard input. Does not echo to the screen."""
    if not sys.stdin.isatty():
        return None
    try:
        # Windows
        import msvcrt

        end_time = time.time() + timeout
        while time.time() < end_time:
            if msvcrt.kbhit():
                return msvcrt.getwch()
            time.sleep(0.01)
        return None
    except ImportError:
        # Unix
        import select
        import termios
        import tty

        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        try:
            tty.setraw(fd)
            rlist, _, _ = select.select([fd], [], [], timeout)
            if rlist:
                ch = sys.stdin.read(1)
                return ch
            else:
                return None
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
