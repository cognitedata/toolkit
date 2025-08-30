import time
from collections.abc import Callable, Iterable
from typing import Any, Literal, NoReturn
from unittest.mock import MagicMock, patch

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
    def __init__(self, raise_in: Literal["iter", "next"] = "next") -> None:
        self.raise_in = raise_in

    def __iter__(self) -> "FailingIterator":
        if self.raise_in == "iter":
            raise TypeError("Error on iter call")
        return self

    def __next__(self) -> NoReturn:
        raise Exception("Error on first call")


def test_error_handling_in_download_worker() -> None:
    executor = ProducerWorkerExecutor(FailingIterator(raise_in="next"), MagicMock(), MagicMock(), 1, 10)
    executor.run()

    # Verify error occurred
    assert executor.error_occurred
    assert "Error on first call" in executor.error_message


def test_error_handling_in_download_worker_on_iter_call() -> None:
    executor = ProducerWorkerExecutor(FailingIterator(raise_in="iter"), MagicMock(), MagicMock(), 1, 10)
    executor.run()

    # Verify error occurred
    assert executor.error_occurred
    assert "Error on iter call" in executor.error_message


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


def test_kill_switch_stops_execution() -> None:
    downloaded: list[list[int]] = []
    to_download = [[1], [2], [3], [4], [5]]

    def slow_download() -> Iterable[list[int]]:
        for item in to_download:
            time.sleep(0.1)
            yield item
            downloaded.append(item)

    def user_input(timeout: float) -> str:
        return "q"

    with patch(f"{ProducerWorkerExecutor.__module__}.getch", user_input):
        executor = ProducerWorkerExecutor[list[list[int]], list[list[int]]](
            slow_download(), lambda x: x, lambda x: x, len(to_download), max_queue_size=2
        )
        executor.run()
        assert executor.stopped_by_user

    assert len(downloaded) < len(to_download)
