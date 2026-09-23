import sys
from collections.abc import Iterable
from pathlib import Path
import subprocess
from typing import Any

if sys.version_info >= (3, 11):
    import tomllib
    from tomllib import TOMLDecodeError
else:
    import tomli as tomllib
    from tomli import TOMLDecodeError

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.identifiers import InternalId
from cognite_toolkit._cdf_tk.exceptions import ResourceCreationError
from cognite_toolkit._cdf_tk.resource_ios._base_ios import FailedReadExtra, ReadExtra, SuccessExtra
from cognite_toolkit._cdf_tk.utils import (
    calculate_directory_hash,
    calculate_hash,
    calculate_zipfile_hash,
    humanize_collection,
)
from cognite_toolkit._cdf_tk.utils.file import (
    create_zip_in_memory,
    sanitize_filename,
    validate_safe_path,
    yaml_safe_dump,
)
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
        validate_safe_path(external_id)
        code_root = filepath.parent.resolve()
        function_rootdir = (filepath.parent / external_id).resolve()
        if not function_rootdir.is_relative_to(code_root):
            raise ValueError(
                f"Invalid function code directory for {external_id!r}: path must remain inside {code_root}"
            )
        return function_rootdir

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
    def _export_requirements(cls, function_rootdir: Path, package: str | None) -> bytes | FailedReadExtra | None:
        requirements_file = function_rootdir / "requirements.txt"
        if requirements_file.is_file():
            return None

        uv_root = next(
            (
                parent
                for parent in ([function_rootdir, *function_rootdir.parents] if package else [function_rootdir])
                if (parent / "pyproject.toml").exists() or (parent / "uv.lock").exists()
            ),
            None,
        )
        if uv_root is None:
            return None
        pyproject_file = uv_root / "pyproject.toml"
        lock_file = uv_root / "uv.lock"
        if not pyproject_file.exists() or not lock_file.exists():
            missing = "pyproject.toml" if not pyproject_file.exists() else "uv.lock"
            return FailedReadExtra(
                code="MISSING",
                error=(
                    f"Function App {function_rootdir.name!r} requires either requirements.txt or both pyproject.toml "
                    f"and uv.lock. Missing {missing}."
                ),
                source_path=uv_root,
            )

        try:
            pyproject = tomllib.loads(pyproject_file.read_text())
        except (OSError, TOMLDecodeError) as error:
            return FailedReadExtra(
                code="SYNTAX-ERROR", error=f"Could not read {pyproject_file}: {error}", source_path=pyproject_file
            )
        if isinstance(pyproject.get("tool"), dict) and isinstance(pyproject["tool"].get("uv"), dict):
            if "workspace" in pyproject["tool"]["uv"] and package is None:
                return FailedReadExtra(
                    code="MISSING",
                    error=(
                        f"Function App {function_rootdir.name!r} uses a UV workspace. Set the FunctionApp YAML "
                        "'package' field to the workspace package to deploy."
                    ),
                    source_path=pyproject_file,
                )

        args = [
            "uv",
            "export",
            "--format",
            "requirements.txt",
            "--frozen",
            "--no-dev",
            "--no-emit-workspace",
            "--no-header",
            "--no-annotate",
            "--no-hashes",
            *(["--package", package] if package else []),
        ]
        try:
            result = subprocess.run(args, cwd=uv_root, capture_output=True, timeout=30, check=False)
        except FileNotFoundError:
            return FailedReadExtra(
                code="MISSING",
                error="Cannot export Function App dependencies because UV is not installed. Install uv or add requirements.txt.",
                source_path=function_rootdir,
            )
        except subprocess.TimeoutExpired:
            return FailedReadExtra(
                code="SYNTAX-ERROR",
                error=f"Timed out exporting Function App dependencies from {function_rootdir} with uv.",
                source_path=function_rootdir,
            )
        except OSError as error:
            return FailedReadExtra(
                code="SYNTAX-ERROR",
                error=f"Could not export Function App dependencies from {function_rootdir}: {error}",
                source_path=function_rootdir,
            )
        if result.returncode:
            stderr = result.stderr.decode(errors="replace").strip()
            return FailedReadExtra(
                code="SYNTAX-ERROR",
                error=(
                    f"UV could not export locked dependencies for Function App {function_rootdir.name!r}. "
                    f"Run 'uv lock' and retry. {stderr[-1000:]}"
                ),
                source_path=function_rootdir,
            )
        return result.stdout

    @classmethod
    def get_extra_files(
        cls,
        filepath: Path,
        external_id: str,
        item: dict[str, Any],
        function_hash_key: str,
        package: str | None = None,
        export_uv_requirements: bool = False,
        hash_build_file: bool = False,
        remove_fields: list[str] | None = None,
    ) -> Iterable[ReadExtra]:
        try:
            function_rootdir = cls.get_code_implicitly(filepath, external_id)
        except ValueError as error:
            yield FailedReadExtra(
                code="SYNTAX-ERROR",
                error=str(error),
                source_path=filepath.parent,
            )
            return
        if not function_rootdir.is_dir():
            yield FailedReadExtra(
                code="MISSING",
                error=f"Cannot find function code for function {external_id!r} in {filepath.as_posix()}. Expected function code directory {function_rootdir.as_posix()} to exist. ",
                source_path=function_rootdir,
            )
            return

        requirements = cls._export_requirements(function_rootdir, package) if export_uv_requirements else None
        if isinstance(requirements, FailedReadExtra):
            yield requirements
            return

        zip_content = create_zip_in_memory(
            function_rootdir,
            additional_content={"requirements.txt": requirements} if isinstance(requirements, bytes) else None,
            exclude_files={"pyproject.toml", "uv.lock"} if isinstance(requirements, bytes) else None,
        )
        function_hash = (
            calculate_zipfile_hash(zip_content, shorten=True)
            if hash_build_file
            else cls.create_hash_values(function_rootdir)
        )
        if not isinstance(item.get("metadata"), dict):
            item["metadata"] = {}
        item["metadata"][function_hash_key] = function_hash
        source_hash = (
            calculate_zipfile_hash(zip_content) if hash_build_file else calculate_directory_hash(function_rootdir)
        )

        yield SuccessExtra(
            source_path=function_rootdir,
            source_hash=source_hash,
            resource_field=None,
            remove_fields=remove_fields or [],
            suffix=".zip",
            content_byte=zip_content,
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
