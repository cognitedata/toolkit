import queue
import threading
from collections.abc import Callable, Iterable, Sized
from typing import Generic, TypeVar

from rich.console import Console
from rich.progress import BarColumn, Progress, TaskID, TaskProgressColumn, TextColumn, TimeRemainingColumn

T_Download = TypeVar("T_Download", bound=Sized)
T_Processed = TypeVar("T_Processed", bound=Sized)


class ProducerWorkerExecutor(Generic[T_Download, T_Processed]):
    """This class manages the execution of a producer-worker pattern with multiple threads.

    It downloads data from an iterable, processes it, and writes the processed data.

    Args:
        download_iterable (Iterable[T_Download]): An iterable that yields chunks of items.
        process (Callable[[T_Download], T_Processed]): A function that processes the chunks of items.
        write (Callable[[T_Processed], None]): A function that writes the processed items. This is typically to
            a file, CDF, or another storage format.
        iteration_count (int): The total number of iterations expected. Note this is not the total number of items,
            but rather the number of chunks of items that will be processed.
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
        iteration_count: int,
        max_queue_size: int,
        download_description: str = "downloading",
        process_description: str = "processing",
        write_description: str = "writing to file",
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

        self.download_complete = False
        self.is_processing = False

        # Queues for managing the flow of data between threads
        # Download -> [process_queue] -> Process -> [write_queue] -> Write
        self.process_queue: queue.Queue[T_Download] = queue.Queue(maxsize=max_queue_size)
        self.write_queue: queue.Queue[T_Processed] = queue.Queue(maxsize=max_queue_size)

        self.total_items = 0
        self.error_occurred = False
        self.error_message = ""

    def run(self) -> None:
        with Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeRemainingColumn(),
            console=self.console,
        ) as progress:
            download_task = progress.add_task(self.download_description.title(), total=self.iteration_count)
            download_thread = threading.Thread(target=self._download_worker, args=(progress, download_task))
            process_task = progress.add_task(self.process_description.title(), total=self.iteration_count)
            process_thread = threading.Thread(target=self._process_worker, args=(progress, process_task))

            write_task = progress.add_task(self.download_description.title(), total=self.iteration_count)
            write_thread = threading.Thread(target=self._write_worker, args=(progress, write_task))

            download_thread.start()
            process_thread.start()
            write_thread.start()

            # Wait for all threads to finish
            download_thread.join()
            process_thread.join()
            write_thread.join()

    def _download_worker(self, progress: Progress, download_task: TaskID) -> None:
        """Worker thread for downloading data."""
        iterator = iter(self._download_iterable)
        while not self.error_occurred:
            try:
                items = next(iterator)
                self.total_items += len(items)
                while not self.error_occurred:
                    try:
                        self.process_queue.put(items, timeout=0.5)
                        progress.update(download_task, advance=1)
                        break  # Exit the loop once the item is successfully added
                    except queue.Full:
                        # Retry until the queue has space
                        continue
            except StopIteration:
                self.download_complete = True
                break
            except Exception as e:
                self.error_occurred = True
                self.error_message = str(e)
                self.console.print(f"[red]Error[/red] occurred while {self.download_description}: {self.error_message}")
                break

    def _process_worker(self, progress: Progress, process_task: TaskID) -> None:
        """Worker thread for processing data."""
        self.is_processing = True
        while (not self.download_complete or not self.process_queue.empty()) and not self.error_occurred:
            try:
                items = self.process_queue.get(timeout=0.5)
                processed_items = self._process(items)
                self.write_queue.put(processed_items)
                progress.update(process_task, advance=1)
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
        while (
            not self.download_complete
            or self.is_processing
            or not self.write_queue.empty()
            or not self.process_queue.empty()
        ) and not self.error_occurred:
            try:
                items = self.write_queue.get(timeout=0.5)
                self._write(items)
                progress.update(write_task, advance=1)
                self.write_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                self.error_occurred = True
                self.error_message = str(e)
                self.console.print(f"[red]Error[/red] occurred while {self.write_description}: {self.error_message}")
                break
