from collections.abc import Callable, Hashable, Iterable, Sequence
from enum import Enum
from pathlib import Path
from typing import TypeVar

from cognite.client.data_classes._base import CogniteResourceList, WriteableCogniteResourceList
from rich.console import Console

from cognite_toolkit._cdf_tk.commands._migrate.data_mapper import DataMapper
from cognite_toolkit._cdf_tk.exceptions import ToolkitFileExistsError
from cognite_toolkit._cdf_tk.storageio import StorageIO
from cognite_toolkit._cdf_tk.storageio._base import (
    T_ID,
    T_CogniteResourceList,
    T_Selector,
    T_WritableCogniteResourceList,
)
from cognite_toolkit._cdf_tk.utils.fileio import Chunk, NDJsonWriter, Uncompressed
from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient, HTTPMessage, ItemIDMessage, SuccessItem
from cognite_toolkit._cdf_tk.utils.producer_worker import ProducerWorkerExecutor
from cognite_toolkit._cdf_tk.utils.progress_tracker import ProgressTracker
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
T_SelectorTarget = TypeVar("T_SelectorTarget", bound=Hashable)
T_CogniteResourceListTarget = TypeVar("T_CogniteResourceListTarget", bound=CogniteResourceList)
T_WritableCogniteResourceListTarget = TypeVar("T_WritableCogniteResourceListTarget", bound=WriteableCogniteResourceList)


class MigrationCommand(ToolkitCommand):
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
        source: StorageIO[T_ID, T_Selector, T_CogniteResourceList, T_WritableCogniteResourceList],
        mapper: DataMapper[T_Selector, T_WritableCogniteResourceList, T_CogniteResourceListTarget],
        target_selected: T_SelectorTarget,
        target: StorageIO[T_ID, T_SelectorTarget, T_CogniteResourceListTarget, T_WritableCogniteResourceListTarget],
        log_dir: Path,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> None:
        if log_dir.exists():
            raise ToolkitFileExistsError(
                f"Log directory {log_dir} already exists. Please remove it or choose another directory."
            )
        log_dir.mkdir(parents=True, exist_ok=False)
        source.validate_auth(["read"], selected)
        target.validate_auth(["write"], target_selected)
        mapper.prepare(selected)

        iteration_count: int | None = None
        total_items = source.count(selected)
        if total_items is not None:
            iteration_count = (total_items // source.chunk_size) + (1 if total_items % source.chunk_size > 0 else 0)

        console = Console()
        tracker = ProgressTracker[T_ID](self.Steps.list())
        with (
            NDJsonWriter(log_dir, kind=f"{source.kind}MigrationIssues", compression=Uncompressed) as log_file,
            HTTPClient(config=source.client.config) as write_client,
        ):
            executor = ProducerWorkerExecutor[T_WritableCogniteResourceList, T_CogniteResourceListTarget](
                download_iterable=self._download_iterable(selected, source, tracker),
                process=self._convert(mapper, tracker, log_file),
                write=self._upload(write_client, target, log_file, tracker, dry_run),
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
        action = "Would migrate" if dry_run else "Migrating"
        console.print(f"{action} {total:,} {source.display_name} to {target.display_name}.")

    def _download_iterable(
        self,
        selected: T_Selector,
        source: StorageIO[T_ID, T_Selector, T_CogniteResourceList, T_WritableCogniteResourceList],
        tracker: ProgressTracker,
    ) -> Iterable[T_WritableCogniteResourceList]:
        for chunk in source.download_iterable(selected):
            for item in chunk:
                tracker.set_progress(source.as_id(item), self.Steps.DOWNLOAD, "success")
            yield chunk

    def _convert(
        self,
        mapper: DataMapper[T_Selector, T_WritableCogniteResourceList, T_CogniteResourceListTarget],
        tracker: ProgressTracker[T_ID],
        log_file: NDJsonWriter,
    ) -> Callable[[T_WritableCogniteResourceList], T_CogniteResourceListTarget]:
        def track_mapping(source: T_WritableCogniteResourceList) -> T_CogniteResourceListTarget:
            target, issues = mapper.map_chunk(source)
            for item in source:
                tracker.set_progress(item.id, step=self.Steps.CONVERT, status="success")
            if issues:
                log_file.write_chunks([issue.dump() for issue in issues])
            return target

        return track_mapping

    def _upload(
        self,
        write_client: HTTPClient,
        target: StorageIO[T_ID, T_SelectorTarget, T_CogniteResourceListTarget, T_WritableCogniteResourceListTarget],
        log_file: NDJsonWriter,
        tracker: ProgressTracker[T_ID],
        dry_run: bool,
    ) -> Callable[[T_CogniteResourceListTarget], None]:
        def upload_items(data_chunk: T_CogniteResourceListTarget) -> None:
            if not data_chunk:
                return None
            results: Sequence[HTTPMessage]
            if dry_run:
                results = [SuccessItem(200, target.as_id(item)) for item in data_chunk]
            else:
                results = target.upload_items_force(data_chunk=data_chunk, http_client=write_client, selector=None)

            issues: list[Chunk] = []
            for item in results:
                if isinstance(item, SuccessItem):
                    tracker.set_progress(item.id, step=self.Steps.UPLOAD, status="success")
                elif isinstance(item, ItemIDMessage):
                    tracker.set_progress(item.id, step=self.Steps.UPLOAD, status="failed")
                if not isinstance(item, SuccessItem):
                    issues.append(item.dump_json())
            if issues:
                log_file.write_chunks(issues)
            return None

        return upload_items
