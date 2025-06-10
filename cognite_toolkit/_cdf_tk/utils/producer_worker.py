import queue
import threading
from collections.abc import Callable, Iterable, Sized
from typing import Generic, TypeVar

from rich.console import Console
from rich.progress import BarColumn, Progress, TaskID, TaskProgressColumn, TextColumn, TimeRemainingColumn

T_Download = TypeVar("T_Download", bound=Sized)
T_Processed = TypeVar("T_Processed", bound=Sized)


class ProducerWorkerExecutor(Generic[T_Download, T_Processed]):
    def __init__(
        self,
        download_iterable: Iterable[T_Download],
        process: Callable[[T_Download], T_Processed],
        write_to_file: Callable[[T_Processed], None],
        iteration_count: int,
        max_queue_size: int,
    ) -> None:
        self._download_iterable = download_iterable
        self.download_complete = False
        self.is_processing = False
        self._write_to_file = write_to_file
        self._process = process
        self.console = Console()
        self.process_queue: queue.Queue[T_Download] = queue.Queue(maxsize=max_queue_size)
        self.file_queue: queue.Queue[T_Processed] = queue.Queue(maxsize=max_queue_size)

        self.iteration_count = iteration_count
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
            download_task = progress.add_task("Downloading", total=self.iteration_count)
            download_thread = threading.Thread(target=self._download_worker, args=(progress, download_task))
            process_task = progress.add_task("Processing", total=self.iteration_count)
            process_thread = threading.Thread(target=self._process_worker, args=(progress, process_task))

            write_task = progress.add_task("Writing to file", total=self.iteration_count)
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
                self.console.print(f"[red]Error[/red] occurred while downloading: {self.error_message}")
                break

    def _process_worker(self, progress: Progress, process_task: TaskID) -> None:
        """Worker thread for processing data."""
        self.is_processing = True
        while (not self.download_complete or not self.process_queue.empty()) and not self.error_occurred:
            try:
                items = self.process_queue.get(timeout=0.5)
                processed_items = self._process(items)
                self.file_queue.put(processed_items)
                progress.update(process_task, advance=1)
                self.process_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                self.error_occurred = True
                self.error_message = str(e)
                self.console.print(f"[red]ErrorError[/red] occurred while processing: {self.error_message}")
                break
        self.is_processing = False

    def _write_worker(self, progress: Progress, write_task: TaskID) -> None:
        """Worker thread for writing data to file."""
        while (
            not self.download_complete
            or self.is_processing
            or not self.file_queue.empty()
            or not self.process_queue.empty()
        ) and not self.error_occurred:
            try:
                items = self.file_queue.get(timeout=0.5)
                self._write_to_file(items)
                progress.update(write_task, advance=1)
                self.file_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                self.error_occurred = True
                self.error_message = str(e)
                self.console.print(f"[red]Error[/red] occurred while writing: {self.error_message}")
                break
