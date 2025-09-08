from collections.abc import Callable, Hashable, Iterable
from enum import Enum
from pathlib import Path
from typing import TypeVar

from cognite.client.data_classes._base import CogniteResourceList, WriteableCogniteResourceList
from rich.console import Console

from cognite_toolkit._cdf_tk.commands._migrate.data_mapper import DataMapper
from cognite_toolkit._cdf_tk.exceptions import ToolkitFileExistsError
from cognite_toolkit._cdf_tk.storageio import StorageIO
from cognite_toolkit._cdf_tk.storageio._base import T_CogniteResourceList, T_Selector, T_WritableCogniteResourceList
from cognite_toolkit._cdf_tk.utils.fileio import NDJsonWriter, Uncompressed
from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient
from cognite_toolkit._cdf_tk.utils.producer_worker import ProducerWorkerExecutor
from cognite_toolkit._cdf_tk.utils.progress_tracker import ProgressTracker

T_SelectorTarget = TypeVar("T_SelectorTarget", bound=Hashable)
T_CogniteResourceListTarget = TypeVar("T_CogniteResourceListTarget", bound=CogniteResourceList)
T_WritableCogniteResourceListTarget = TypeVar("T_WritableCogniteResourceListTarget", bound=WriteableCogniteResourceList)


class MigrationCommand:
    class Steps(str, Enum):
        DOWNLOAD = "download"
        CONVERT = "convert"
        UPLOAD = "upload"

        @classmethod
        def list(cls) -> list[str]:
            return [step.value for step in cls.__members__.values()]

    def migrate(
        self,
        selected: T_Selector,
        source: StorageIO[T_Selector, T_CogniteResourceList, T_WritableCogniteResourceList],
        mapper: DataMapper[T_Selector, T_WritableCogniteResourceList, T_CogniteResourceListTarget],
        target: StorageIO[T_SelectorTarget, T_CogniteResourceListTarget, T_WritableCogniteResourceListTarget],
        # source: StorageIO[T_Selector, AssetWriteList, AssetList],
        # mapper: DataMapper[T_Selector, AssetList, NodeApplyList],
        # target: StorageIO[T_SelectorTarget, NodeApplyList, NodeList],
        log_dir: Path,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> None:
        if log_dir.exists():
            raise ToolkitFileExistsError(
                f"Log directory {log_dir} already exists. Please remove it or choose another directory."
            )
        log_dir.mkdir(parents=True, exist_ok=False)
        # source.validate_access("READ")
        # target.validate_access("WRITE")
        mapper.prepare(selected)

        iteration_count: int | None = None
        total_items = source.count(selected)
        if total_items is not None:
            iteration_count = (total_items // source.chunk_size) + (1 if total_items % source.chunk_size > 0 else 0)

        console = Console()
        tracker = ProgressTracker[Hashable](self.Steps.list())
        with (
            NDJsonWriter(log_dir, kind=f"{source.kind}MigrationIssues", compression=Uncompressed) as log_file,
            HTTPClient(config=source.client.config) as write_client,
        ):
            executor = ProducerWorkerExecutor[T_WritableCogniteResourceList, T_CogniteResourceListTarget](
                download_iterable=self._download_iterable(selected, source, tracker),
                process=self._convert(mapper, tracker, log_file),
                write=self._upload(write_client, target, log_file, dry_run),
                iteration_count=iteration_count,
                max_queue_size=10,
                download_description=f"Downloading {source.display_name}",
                process_description=f"Converting {source.display_name} to {target.display_name}",
                write_description=f"Uploading to {target.display_name}",
                console=console,
            )

            executor.run()
            total = executor.total_items

        executor.raise_on_error()
        # self.print_table(tracker, console=console)
        action = "Would migrate" if dry_run else "Migrating"
        console.print(f"{action} {total:,} {source.display_name} to {target.display_name}.")

    def _download_iterable(
        self,
        selected: T_Selector,
        source: StorageIO[T_Selector, T_CogniteResourceList, T_WritableCogniteResourceList],
        tracker: ProgressTracker,
    ) -> Iterable[T_WritableCogniteResourceList]:
        # for chunk in source.download_iterable(selected):
        #     for item in chunk:
        #         tracker.set_progress(source.as_id(item), self.Steps.DOWNLOAD, "success")
        #     yield chunk
        raise NotImplementedError()

    def _convert(
        self,
        conversion: DataMapper[T_Selector, T_WritableCogniteResourceList, T_CogniteResourceListTarget],
        tracker: ProgressTracker,
        log_file: NDJsonWriter,
    ) -> Callable[[T_WritableCogniteResourceList], T_CogniteResourceListTarget]:
        raise NotImplementedError()

    def _upload(
        self,
        write_client: HTTPClient,
        target: StorageIO[T_SelectorTarget, T_CogniteResourceListTarget, T_WritableCogniteResourceListTarget],
        log_file: NDJsonWriter,
        dry_run: bool,
    ) -> Callable[[T_CogniteResourceListTarget], None]:
        raise NotImplementedError()
