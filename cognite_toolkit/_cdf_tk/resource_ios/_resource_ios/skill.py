from collections.abc import Hashable, Iterable, Sequence
from pathlib import Path
from typing import Any, Literal, final

from pydantic import ValidationError

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.group import (
    AclType,
    AgentsAcl,
    AllScope,
    ScopeDefinition,
)
from cognite_toolkit._cdf_tk.client.resource_classes.skill import SkillRequest, SkillResponse
from cognite_toolkit._cdf_tk.constants import BUILD_FOLDER_ENCODING
from cognite_toolkit._cdf_tk.exceptions import ToolkitFileNotFoundError, ToolkitValueError
from cognite_toolkit._cdf_tk.resource_ios._base_ios import FailedReadExtra, ReadExtra, ResourceIO, SuccessExtra
from cognite_toolkit._cdf_tk.utils import (
    calculate_hash,
    humanize_collection,
    load_yaml_inject_variables,
    safe_read,
    sanitize_filename,
)
from cognite_toolkit._cdf_tk.yaml_classes import SkillYAML
from cognite_toolkit._cdf_tk.yaml_classes.skill import SkillMarkdown

_SKILL_MD_SUFFIX = ".Skill.md"


@final
class SkillIO(ResourceIO[ExternalId, SkillRequest, SkillResponse]):
    folder_name = "agents"
    resource_cls = SkillResponse
    resource_write_cls = SkillRequest
    kind = "Skill"
    yaml_cls = SkillYAML
    _doc_base_url = ""
    _doc_url = "https://api-docs.cognite.com/20230101-beta/tag/Skills/operation/skill_create_ai_skills_post/"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._source_file_by_external_id: dict[str, Path] = {}

    @classmethod
    def get_id(cls, item: SkillRequest | SkillResponse | dict) -> ExternalId:
        if isinstance(item, dict):
            external_id = item.get("externalId") or item.get("external_id")
            if external_id is None:
                raise KeyError("externalId")
            return ExternalId(external_id=external_id)
        return ExternalId(external_id=item.external_id)

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def as_str(cls, id: ExternalId) -> str:
        return sanitize_filename(id.external_id)

    @classmethod
    def get_minimum_scope(cls, items: Sequence[SkillRequest]) -> ScopeDefinition:
        return AllScope()

    @classmethod
    def create_acl(cls, actions: set[Literal["READ", "WRITE"]], scope: ScopeDefinition) -> Iterable[AclType]:
        if isinstance(scope, AllScope):
            yield AgentsAcl(actions=sorted(actions), scope=scope)

    @classmethod
    def _get_skill_sidecar_candidates(
        cls, filepath: Path, external_id: str, has_explicit_external_id: bool
    ) -> list[Path]:
        candidates = [
            filepath.with_suffix(".md"),
            filepath.parent / external_id / "SKILL.md",
            filepath.parent / f"{external_id}.{cls.kind}.md",
        ]
        if not has_explicit_external_id:
            return candidates
        # For multi-item YAML files, prefer item-specific sidecars over shared sibling markdown.
        return [*candidates[1:], candidates[0]]

    @classmethod
    def _get_skill_output_sidecar_path(cls, filepath: Path, external_id: str) -> Path:
        # Normalize to explicit-id-specific sidecars for pull output.
        candidates = cls._get_skill_sidecar_candidates(
            filepath=filepath, external_id=external_id, has_explicit_external_id=True
        )
        explicit_candidates = candidates[:2]
        return next((path for path in explicit_candidates if path.exists()), filepath.parent / external_id / "SKILL.md")

    @classmethod
    def get_extra_files(cls, filepath: Path, identifier: ExternalId, item: dict[str, Any]) -> Iterable[ReadExtra]:
        if "content" in item:
            return

        prefix = item.get("externalId") or item.get("external_id") or identifier.external_id or filepath.stem
        candidates = cls._get_skill_sidecar_candidates(
            filepath=filepath,
            external_id=str(prefix),
            has_explicit_external_id=bool(item.get("externalId") or item.get("external_id")),
        )
        skill_md_path = next((p for p in candidates if p.exists()), None)
        if skill_md_path is None:
            yield FailedReadExtra(
                source_path=filepath,
                code="MISSING",
                error=(
                    f"Missing skill content for {prefix!r} in {filepath.as_posix()}. "
                    f"No 'content' field found and no <prefix>.md file found. Expected one of: "
                    f"{humanize_collection(candidates)}"
                ),
            )
            return
        content = safe_read(skill_md_path, encoding=BUILD_FOLDER_ENCODING)
        try:
            # triggers validation error if invalid
            SkillMarkdown.load(content)
        except (ValidationError, ValueError) as e:
            yield FailedReadExtra(
                source_path=skill_md_path,
                code="SYNTAX-ERROR",
                error=f"\nInvalid markdown in {skill_md_path.parent.name}/{skill_md_path.name}.\n{e}",
            )
            return

        source_hash = calculate_hash(content, shorten=True)
        yield SuccessExtra(
            source_path=skill_md_path,
            source_hash=source_hash,
            suffix=_SKILL_MD_SUFFIX,
            content=content,
            description="skill instructions",
        )

    def load_resource_file(
        self, filepath: Path, environment_variables: dict[str, str | None] | None = None
    ) -> list[dict[str, Any]]:
        resources = load_yaml_inject_variables(
            self.safe_read(filepath),
            environment_variables or {},
            original_filepath=filepath,
        )
        raw_list = resources if isinstance(resources, list) else [resources]
        for item in raw_list:
            external_id = self.get_id(item).external_id
            self._source_file_by_external_id[external_id] = filepath
        return raw_list

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> SkillRequest:
        external_id = self.get_id(resource).external_id
        source_file = self._source_file_by_external_id.get(external_id)
        if source_file is None:
            raise ToolkitValueError(f"Missing source file mapping for skill {external_id!r}")
        content = resource.get("content")
        if content is None:
            candidates = self._get_skill_sidecar_candidates(
                filepath=source_file,
                external_id=external_id,
                has_explicit_external_id=bool(resource.get("externalId") or resource.get("external_id")),
            )
            sidecar_path = next((p for p in candidates if p.exists()), None)
            if sidecar_path is None:
                raise ToolkitFileNotFoundError(
                    (f"Missing skill content for {external_id!r}. Expected one of: {humanize_collection(candidates)}"),
                    source_file,
                )
            content = safe_read(sidecar_path, encoding=BUILD_FOLDER_ENCODING)
        try:
            markdown = SkillMarkdown.load(content)
        except (ValidationError, ValueError) as error:
            raise ToolkitValueError(f"Invalid markdown for skill {external_id!r}: {error}") from error

        request_payload = {
            "externalId": external_id,
            "name": markdown.name,
            "description": markdown.description,
            "content": markdown.content,
        }
        return self.resource_write_cls._load(request_payload)

    def dump_resource(self, resource: SkillResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        return resource.as_request_resource().dump()

    def split_resource(
        self, base_filepath: Path, resource: dict[str, Any]
    ) -> Iterable[tuple[Path, dict[str, Any] | str]]:
        content = resource.pop("content", None)
        # Keep YAML schema minimal (externalId only).
        resource.pop("name", None)
        resource.pop("description", None)
        if isinstance(content, str):
            external_id = self.get_id(resource).external_id
            sidecar_path = self._get_skill_output_sidecar_path(base_filepath, external_id)
            yield sidecar_path, content
        yield base_filepath, resource

    def create(self, items: Sequence[SkillRequest]) -> list[SkillResponse]:
        return self.client.tool.skills.create(items)

    def retrieve(self, ids: Sequence[ExternalId]) -> list[SkillResponse]:
        return self.client.tool.skills.retrieve(list(ids), ignore_unknown_ids=True)

    def update(self, items: Sequence[SkillRequest]) -> list[SkillResponse]:
        return self.client.tool.skills.update(items)

    def delete(self, ids: Sequence[ExternalId]) -> int:
        self.client.tool.skills.delete(list(ids), ignore_unknown_ids=True)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[SkillResponse]:
        return self.client.tool.skills.list(limit=None)
