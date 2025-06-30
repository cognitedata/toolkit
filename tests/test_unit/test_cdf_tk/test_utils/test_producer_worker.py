import time
from typing import Any, Callable
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.utils.producer_worker import ProducerWorkerExecutor


@pytest.fixture
def setup_executor() -> tuple[
    ProducerWorkerExecutor, Callable[[list[int]], list[int]], Callable[[list[int]], None], int
]:
    download_iterable = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    process: MagicMock = MagicMock(side_effect=lambda x: [i * 2 for i in x])
    write_to_file: MagicMock = MagicMock()

    executor: ProducerWorkerExecutor = ProducerWorkerExecutor(
        download_iterable=download_iterable,
        process=process,
        write=write_to_file,
        iteration_count=len(download_iterable),
        max_queue_size=2,
    )
    return executor, process, write_to_file, len(download_iterable)


def test_run_success(setup_executor: tuple[ProducerWorkerExecutor, MagicMock, MagicMock, int]) -> None:
    executor, process, write_to_file, iteration_count = setup_executor
    executor.run()

    # Verify process was called for each batch
    assert process.call_count == iteration_count

    # Verify write_to_file was called for each processed batch
    assert write_to_file.call_count == iteration_count


def test_download_worker_handles_full_queue(monkeypatch: Any) -> None:
    max_queue_size = 1

    def process(items: list[int]) -> list[int]:
        # Simulate slow processing time
        time.sleep(1)
        return items

    def write_to_file(items: list[int]) -> None:
        nonlocal written
        written.append(items)

    to_download = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    written: list[list[int]] = []

    executor = ProducerWorkerExecutor(
        to_download, MagicMock(side_effect=process), MagicMock(side_effect=write_to_file), 2, max_queue_size
    )

    # Run the executor
    executor.run()

    assert written == to_download


class FailingIterator:
    def __init__(self):
        self.first_call = True

    def __iter__(self):
        return self

    def __next__(self):
        raise Exception("Error on first call")


def test_error_handling_in_download_worker() -> None:
    executor = ProducerWorkerExecutor(FailingIterator(), MagicMock(), MagicMock(), 1, 10)
    executor.run()

    # Verify error occurred
    assert executor.error_occurred
    assert "Error on first call" in executor.error_message


def test_error_handling_in_process_worker(
    setup_executor: tuple[ProducerWorkerExecutor, MagicMock, MagicMock, int],
) -> None:
    executor, process, _, _ = setup_executor
    process.side_effect = Exception("Process error")
    executor.run()

    # Verify error occurred
    assert executor.error_occurred
    assert "Process error" in executor.error_message


def test_error_handling_in_write_worker(
    setup_executor: tuple[ProducerWorkerExecutor, MagicMock, MagicMock, int],
) -> None:
    executor, _, write_to_file, _ = setup_executor
    write_to_file.side_effect = Exception("Write error")
    executor.run()

    # Verify error occurred
    assert executor.error_occurred
    assert "Write error" in executor.error_message
