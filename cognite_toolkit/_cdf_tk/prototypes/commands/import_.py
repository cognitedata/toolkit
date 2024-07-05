from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
from pathlib import Path
from typing import Any

import yaml
from cognite.client import CogniteClient
from rich import print
from rich.table import Table

from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.exceptions import AuthenticationError, ToolkitValueError
from cognite_toolkit._cdf_tk.tk_warnings import LowSeverityWarning
from cognite_toolkit._cdf_tk.utils import read_yaml_file


class ImportTransformationCLI(ToolkitCommand):
    def __init__(
        self,
        get_client: Callable[[], CogniteClient] | None = None,
        print_warning: bool = True,
        skip_tracking: bool = False,
    ):
        super().__init__(print_warning, skip_tracking)
        self._dataset_external_id_by_id: dict[int, str] = {}
        # We only initialize the client if we need to look up dataset ids.
        self._client: CogniteClient | None = None
        self._get_client = get_client

    def execute(
        self,
        source: Path,
        destination: Path,
        overwrite: bool,
        flatten: bool,
        verbose: bool = False,
    ) -> None:
        # Manifest files are documented at
        # https://cognite-transformations-cli.readthedocs-hosted.com/en/latest/quickstart.html#transformation-manifest
        if source.is_file() and source.suffix in {".yaml", ".yml"}:
            yaml_files = [source]
        elif source.is_file():
            raise ToolkitValueError(f"File {source} is not a YAML file.")
        elif source.is_dir():
            yaml_files = list(source.rglob("*.yaml")) + list(source.rglob("*.yml"))
        else:
            raise ToolkitValueError(f"Source {source} is not a file or directory.")

        if not yaml_files:
            self.warn(LowSeverityWarning("No YAML files found in the source directory."))
            return None

        count_by_resource_type: dict[str, int] = defaultdict(int)
        for yaml_file in yaml_files:
            data = self._load_file(yaml_file)
            if data is None:
                continue

            # The convert schedule and notifications pop off the schedule and notifications
            # keys from the transformation
            schedule = self._convert_schedule(data, data["externalId"], yaml_file)
            notifications = self._convert_notifications(data, data["externalId"], yaml_file)
            transformation, source_query_relative_path = self._convert_transformation(data, yaml_file)
            source_query_path = yaml_file.parent / source_query_relative_path if source_query_relative_path else None

            if source_query_path and not source_query_path.exists():
                raise ToolkitValueError(
                    f"Query file {source_query_path.as_posix()!r} does not exist. "
                    f"This is referenced in {yaml_file.as_posix()!r}."
                )

            if flatten:
                destination_folder = destination
            else:
                destination_folder = destination / yaml_file.relative_to(source).parent
            destination_folder.mkdir(parents=True, exist_ok=True)

            destination_transformation = destination_folder / f"{yaml_file.stem}.Transformation.yaml"
            if not overwrite and destination_transformation.exists():
                self.warn(LowSeverityWarning(f"File already exists at {destination_transformation}. Skipping."))
                continue
            destination_transformation.write_text(yaml.safe_dump(transformation))
            if source_query_path is not None:
                destination_query_path = destination_folder / f"{destination_transformation.stem}.sql"
                destination_query_path.write_text(source_query_path.read_text())

            if schedule is not None:
                destination_schedule = destination_folder / f"{yaml_file.stem}.Schedule.yaml"
                destination_schedule.write_text(yaml.safe_dump(schedule))
            if notifications:
                destination_notification = destination_folder / f"{yaml_file.stem}.Notification.yaml"
                destination_notification.write_text(yaml.safe_dump(notifications))
            if verbose:
                print(f"Imported {yaml_file} to {destination_folder}.")
            count_by_resource_type["transformation"] += 1
            count_by_resource_type["schedule"] += 1 if schedule is not None else 0
            count_by_resource_type["notification"] += len(notifications)

        print(f"Finished importing from {source} to {destination}.")
        table = Table(title="Import transformation-cli Summary")
        table.add_column("Resource Type", justify="right", style="cyan")
        table.add_column("Count", justify="right", style="magenta")
        for resource_type, count in count_by_resource_type.items():
            table.add_row(resource_type, str(count))
        print(table)

    def _load_file(self, yaml_file: Path) -> dict[str, Any] | None:
        content = read_yaml_file(yaml_file, expected_output="dict")
        required_keys = {"externalId", "name", "destination", "query"}
        if missing_keys := required_keys - content.keys():
            self.warn(
                LowSeverityWarning(
                    f"Missing required keys {missing_keys} in {yaml_file}. Likely not a Transformation manifest. Skipping."
                )
            )
            return None
        return content

    def _convert_transformation(
        self, transformation: dict[str, Any], source_file: Path
    ) -> tuple[dict[str, Any], Path | None]:
        if "shared" in transformation:
            transformation["isPublic"] = transformation.pop("shared")
        if "action" in transformation:
            transformation["conflictMode"] = transformation.pop("action")
        if "ignoreNullFields" not in transformation:
            # This is required by the API, but the transformation-cli sets it to true by default.
            transformation["ignoreNullFields"] = True
        source_query_path: Path | None = None
        if isinstance(transformation["query"], dict):
            query = transformation.pop("query")
            if "file" in query:
                source_query_path = Path(query.pop("file"))

        if "dataSetId" in transformation:
            if "dataSetExternalId" in transformation:
                self.warn(LowSeverityWarning(f"Both dataSetId and dataSetExternalId are present in {source_file}."))
            else:
                data_set_external_id = self._lookup_dataset(transformation.pop("dataSetId"))
                if data_set_external_id is None:
                    self.warn(
                        LowSeverityWarning(f"Failed to find DataSet with id {transformation['dataSetId']} in CDF.")
                    )
                else:
                    transformation["dataSetExternalId"] = data_set_external_id

        if "authentication" in transformation:
            authentication = transformation["authentication"]
            if not isinstance(authentication, dict):
                self.warn(LowSeverityWarning(f"Invalid authentication format in {source_file}."))
            else:
                if "tokenUrl" in authentication:
                    authentication["tokenUri"] = authentication.pop("tokenUrl")

                if "read" in authentication or "write" in authentication:
                    # Read or Write in authentication.
                    transformation.pop("authentication")

                read = authentication.pop("read", None)
                if read and isinstance(read, dict):
                    if "tokenUrl" in read:
                        read["tokenUri"] = read.pop("tokenUrl")
                    transformation["sourceOidcCredentials"] = read

                write = authentication.pop("write", None)
                if write and isinstance(write, dict):
                    if "tokenUrl" in write:
                        write["tokenUri"] = write.pop("tokenUrl")
                    transformation["destinationOidcCredentials"] = write
        return transformation, source_query_path

    def _convert_notifications(
        self, transformation: dict[str, Any], external_id: str, source_file: Path
    ) -> list[dict[str, Any]]:
        notifications = []
        if "notifications" in transformation:
            notifications_raw = transformation.pop("notifications")
            if isinstance(notifications_raw, list) and all(isinstance(n, str) for n in notifications_raw):
                notifications = [
                    {"destination": email, "transformationExternalId": external_id} for email in notifications_raw
                ]
            else:
                self.warn(LowSeverityWarning(f"Invalid notifications format in {source_file}."))
        return notifications

    def _convert_schedule(
        self, transformation: dict[str, Any], external_id: str, source_file: Path
    ) -> dict[str, Any] | None:
        schedule: dict[str, Any] | None = None
        if "schedule" in transformation:
            schedule_raw = transformation.pop("schedule")
            if isinstance(schedule_raw, str):
                schedule = {"interval": schedule_raw}
            elif isinstance(schedule_raw, dict):
                schedule = schedule_raw
            else:
                self.warn(LowSeverityWarning(f"Invalid schedule format in {source_file}."))
            if isinstance(schedule, dict):
                schedule["externalId"] = external_id
        return schedule

    def _lookup_dataset(self, dataset_id: int) -> str | None:
        if dataset_id in self._dataset_external_id_by_id:
            return self._dataset_external_id_by_id[dataset_id]
        dataset = self.client.data_sets.retrieve(id=dataset_id)
        if dataset is None or dataset.external_id is None:
            return None
        self._dataset_external_id_by_id[dataset.id] = dataset.external_id
        return dataset.external_id

    @property
    def client(self) -> CogniteClient:
        if self._client is None:
            if self._get_client is None:
                raise AuthenticationError(
                    "No Cognite Client available. Are you missing a .env file?"
                    "\nThis is required to look up dataset ids in "
                    "the transformation-cli manifest(s)."
                )
            self._client = self._get_client()
        return self._client
