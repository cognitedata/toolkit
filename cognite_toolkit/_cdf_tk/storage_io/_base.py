from abc import ABC, abstractmethod
from pathlib import Path

from cognite.client.data_classes import RowList
from rich.console import Console

from cognite_toolkit._cdf_tk import storage_io
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.raw import RawTable
from cognite_toolkit._cdf_tk.tk_warnings import LowSeverityWarning
from cognite_toolkit._cdf_tk.utils.file import yaml_safe_dump
from cognite_toolkit._cdf_tk.utils.producer_worker import ProducerWorkerExecutor
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal
from cognite_toolkit._cdf_tk.utils.records import RecordReader, RecordWriter

class StorageIO(ABC):
    """"""
    folder_name: str
    kind: str
    display_name: str
    supported_download_formats: frozenset[str]
    supported_compressions: frozenset[str]
    supported_read_formats: frozenset[str]

    @abstractmethod
    def get_identifiers(self) -> list[T_StorageID]:
        raise NotImplementedError()

    @abstractmethod
    def download_iterable(self, identifier: T_StorageID, limit: int) -> Iterable[T_DataType]:
        raise NotImplementedError()

    @abstractmethod
    def upload_items(self, data_chunk: T_DataType) -> None:
        raise NotImplementedError()

    @abstractmethod
    def data_to_json_chunk(self, data_chunk: T_DataType) -> list[dict[str, JsonVal]]:
        raise NotImplementedError()

    @abstractmethod
    def json_chunk_to_data(self, data_chunk: list[dict[str, JsonVal]]) -> T_DataType:
        raise NotImplementedError()


class RawStorageIO(StorageIO):
    def download(self, identifier: RawTable, limit) -> Iterable[Raw]
        return client.raw.rows(
            db_name=identifier.db_name, table_name=identifier.table_name, chunk_size=10_000, limit=limit, partitions=8
        )

    def data_to_json_chunk(self, data_chunk: RowList) -> list[dict[str, JsonVal]]:
        return data_chunk.as_write().dump()

    def json_chunk_to_data(self, data_chunk: list[dict[str, JsonVal]]) -> RowList:
        return RowList._load(data_chunk)


def download(io: StorageIO, output_dir: Path, verbose: bool, format: str, compression: str, limit: int = 100_000) -> None:
    identifiers = io.get_identifiers()
    target_directory = output_dir / io.folder_name
    target_directory.mkdir(parents=True, exist_ok=True)

    console = Console()
    for identifier in identifiers:
        if verbose:
            console.print(f"Downloading {io.display_name} '{identifier!s}' to {output_dir.as_posix()!r}")
        data_filepath = io.create_filepath(identifier, format, compression)
        if data_filepath.exists():
            self.warn(LowSeverityWarning(f"File {data_filepath.as_posix()!r} already exists, skipping download."))
            continue
        io.dump_identifier(identifier, data_filepath)
        iteration_count = 10
        with RecordWriter(data_filepath, str(format_), str(compression)) as writer:  # type: ignore[arg-type]
            executor = ProducerWorkerExecutor[RowList, list[dict[str, JsonVal]]](
                download_iterable=io.download_iterable(identifier, limit),
                process=io.json_chunk_to_data,
                write=writer.write_records,
                iteration_count=iteration_count,
                max_queue_size=8 * 5,
                download_description=f"Downloading {identifier!s}",
                process_description=f"Processing",
                write_description=f"Writing {data_filepath.name!r}",
                console=console,
            )
            executor.run()

            if executor.error_occurred:
                raise ValueError(
                    "An error occurred during the download process: " + executor.error_message
                )

        console.print(
            f"Downloaded {identifier!s} to {data_filepath.as_posix()!r}"
        )

def upload(io: StorageIO, input_dir: Path, verbose: bool) -> None:
    files = input_dir.glob(f"*.{io.kind}.*")
    identifiers = io.get_identifiers(files)

    console = Console()
    for file in files:
        if verbose:
            console.print(f"Uploading {io.display_name} from {file.as_posix()!r}")

        line_count = 10
        with RecordReader(file) as reader:
            iteration_count = (line_count // io.chunk_size) + 1
            executor = ProducerWorkerExecutor[list[dict[str, JsonVal]], RowList](
                download_iterable=reader.read_records_in_chunks(chunk_size=io.chunk_size),
                process=io.json_chunk_to_data,
                write=io.upload_items,
                iteration_count=iteration_count,
                max_queue_size=8 * 5,
                download_description=f"Reading {file.as_posix()!s}",
                process_description=f"Processing",
                write_description=f"Uploading {io.display_name!r}",
                console=console,
            )
            executor.run()
            if executor.error_occurred:
                raise ValueError(
                    "An error occurred during the upload process: " + executor.error_message
                )


