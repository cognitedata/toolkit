from collections.abc import Iterable
from pathlib import Path
from typing import Any

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.identifiers import InternalId
from cognite_toolkit._cdf_tk.exceptions import ResourceCreationError
from cognite_toolkit._cdf_tk.resource_ios._base_ios import FailedReadExtra, ReadExtra, SuccessExtra
from cognite_toolkit._cdf_tk.utils import calculate_directory_hash, calculate_hash, humanize_collection
from cognite_toolkit._cdf_tk.utils.file import create_zip_in_memory, sanitize_filename, yaml_safe_dump
from cognite_toolkit._cdf_tk.yaml_classes import CogniteFileYAML, FileMetadataYAML

from ._file import CogniteFileCRUD, FileMetadataCRUD


class FunctionCodeBundle:
    """Shared handling of function source code bundles and their file resources."""

    metadata_value_limit = 512

    def __init__(self, client: ToolkitClient):
        self.client = client
        self.filemetadata_path_by_external_id: dict[str, Path] = {}
        self.cognitefile_path_by_external_id: dict[str, Path] = {}

    @staticmethod
    def get_code_implicitly(filepath: Path, external_id: str) -> Path:
        return filepath.parent / external_id

    @classmethod
    def create_hash_values(cls, function_rootdir: Path) -> str:
        root_hash = calculate_directory_hash(
            function_rootdir, exclude_prefixes={".DS_Store"}, ignore_files={".pyc"}, shorten=True
        )
        hash_value = f"/={root_hash}"
        to_search = [function_rootdir]
        while to_search:
            search_dir = to_search.pop()
            for file in sorted(search_dir.glob("*"), key=lambda x: x.relative_to(function_rootdir).as_posix()):
                if file.is_dir():
                    to_search.append(file)
                    continue
                elif (file.is_file() and file.suffix == ".pyc") or (file.is_file() and file.name == ".DS_Store"):
                    continue
                file_hash = calculate_hash(file, shorten=True)
                new_entry = f"{file.relative_to(function_rootdir).as_posix()}={file_hash}"
                if len(hash_value) + len(new_entry) > (cls.metadata_value_limit - 1):
                    break
                hash_value += f";{new_entry}"
        return hash_value

    def map_sidecar_paths(self, filepath: Path, filestem: str, external_ids: Iterable[str]) -> None:
        filemetadata = filepath.parent / f"{filestem}.{FileMetadataCRUD.kind}.yaml"
        cognitefile = filepath.parent / f"{filestem}.{CogniteFileCRUD.kind}.yaml"
        for external_id in external_ids:
            if filemetadata.exists():
                self.filemetadata_path_by_external_id[external_id] = filemetadata
            elif cognitefile.exists():
                self.cognitefile_path_by_external_id[external_id] = cognitefile

    @classmethod
    def get_extra_files(
        cls, filepath: Path, external_id: str, item: dict[str, Any], function_hash_key: str
    ) -> Iterable[ReadExtra]:
        function_rootdir = cls.get_code_implicitly(filepath, external_id)
        if not function_rootdir.is_dir():
            yield FailedReadExtra(
                code="MISSING",
                error=f"Cannot find function code for function {external_id!r} in {filepath.as_posix()}. Expected function code directory {function_rootdir.as_posix()} to exist. ",
                source_path=function_rootdir,
            )
            return

        function_hash = cls.create_hash_values(function_rootdir)
        if "metadata" not in item:
            item["metadata"] = {}
        item["metadata"][function_hash_key] = function_hash
        source_hash = calculate_directory_hash(function_rootdir)

        yield SuccessExtra(
            source_path=function_rootdir,
            source_hash=source_hash,
            resource_field=None,
            suffix=".zip",
            content_byte=create_zip_in_memory(function_rootdir),
            description="function code",
            write_to_build=True,
        )
        name = item.get("name")
        if not isinstance(name, str):
            yield FailedReadExtra(
                source_path=function_rootdir,
                code="MISSING",
                error=f"Cannot find function name for function {external_id!r} in {filepath.as_posix()}. This is required and is necessary for creating the function code.",
            )
            return
        filename = sanitize_filename(name)
        if data_set_external_id := item.get("dataSetExternalId"):
            yield SuccessExtra(
                source_path=function_rootdir,
                source_hash=source_hash,
                suffix=f".{FileMetadataCRUD.kind}.yaml",
                content=yaml_safe_dump(
                    FileMetadataYAML(
                        name=f"{filename}.zip",
                        externalId=external_id,
                        dataSetExternalId=data_set_external_id,
                        mimeType="application/zip",
                    ).model_dump(by_alias=True, exclude_unset=True)
                ),
                description="metadata for function code",
                resource_field=None,
                write_to_build=True,
            )
        elif space := item.get("space"):
            yield SuccessExtra(
                source_path=function_rootdir,
                source_hash=source_hash,
                suffix=f".{CogniteFileCRUD.kind}.yaml",
                content=yaml_safe_dump(
                    CogniteFileYAML(
                        space=space,
                        externalId=external_id,
                        name=name,
                        mimeType="application/zip",
                    ).model_dump(by_alias=True, exclude_unset=True)
                ),
                description="metadata for function code",
                resource_field=None,
                write_to_build=True,
            )
        else:
            yield FailedReadExtra(
                source_path=function_rootdir,
                code="MISSING",
                error=f"Failed to create function code metadata for function {external_id!r} in {filepath.as_posix()}. This is required for creating the function code. The function must have either a dataSetExternalId or a space specified.",
            )

    def as_file_by_external_id(self, external_ids: Iterable[str]) -> tuple[dict[Path, str], dict[Path, str]]:
        filemetadata_files: dict[Path, str] = {}
        cognite_files: dict[Path, str] = {}
        missing: list[str] = []
        for external_id in external_ids:
            if filemetadata_path := self.filemetadata_path_by_external_id.get(external_id):
                filemetadata_files[filemetadata_path] = external_id
            elif cognitefile_path := self.cognitefile_path_by_external_id.get(external_id):
                cognite_files[cognitefile_path] = external_id
            else:
                missing.append(external_id)
        if missing:
            raise ResourceCreationError(
                f"Failed to create functions. Missing function code files for {humanize_collection(missing)}"
            )
        return cognite_files, filemetadata_files

    def upload_files(
        self, cognite_files: dict[Path, str], filemetadata_files: dict[Path, str]
    ) -> dict[str, InternalId]:
        file_id_by_external_id: dict[str, InternalId] = {}
        if filemetadata_files:
            fileio = FileMetadataCRUD(self.client, None, None)
            file_request = fileio.load_resource_files(list(filemetadata_files))
            fileresponse = fileio.create(file_request)
            file_id_by_external_id.update(zip(filemetadata_files.values(), (InternalId(id=f.id) for f in fileresponse)))
        if cognite_files:
            cognitefileio = CogniteFileCRUD(self.client, None, None)
            cognitefile_request = cognitefileio.load_resource_files(list(cognite_files))
            cognitefile_response = cognitefileio.create(cognitefile_request)
            dm_fileresponse = self.client.tool.filemetadata.retrieve(
                [node.as_instance_id() for node in cognitefile_response]
            )
            file_id_by_external_id.update(zip(cognite_files.values(), (InternalId(id=f.id) for f in dm_fileresponse)))
        return file_id_by_external_id
