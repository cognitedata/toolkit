from collections.abc import Hashable, Iterable, Sequence
from pathlib import Path
from typing import Any, Literal, final

import yaml
from pydantic import BaseModel

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.group import (
    AclType,
    AgentsAcl,
    AllScope,
    ScopeDefinition,
)
from cognite_toolkit._cdf_tk.client.resource_classes.skill import SkillRequest, SkillResponse
from cognite_toolkit._cdf_tk.constants import BUILD_FOLDER_ENCODING
from cognite_toolkit._cdf_tk.exceptions import ToolkitFileNotFoundError
from cognite_toolkit._cdf_tk.resource_ios._base_ios import FailedReadExtra, ReadExtra, ResourceIO, SuccessExtra
from cognite_toolkit._cdf_tk.utils import (
    calculate_hash,
    humanize_collection,
    load_yaml_inject_variables,
    safe_read,
    sanitize_filename,
)
from cognite_toolkit._cdf_tk.yaml_classes import SkillYAML
from cognite_toolkit._cdf_tk.yaml_classes.skill import SKILL_MD_CONTENT_RE

_SKILL_MD_SUFFIX = ".Skill.md"


class Markdown(BaseModel):
    name: str | None = None
    description: str | None = None
    content: str | None = None

    @classmethod
    def from_markdown(cls, raw: str) -> "Markdown":
        name = None
        description = None
        content = None
        match = SKILL_MD_CONTENT_RE.match(raw)
        if match:
            frontmatter = yaml.safe_load(match.group(1))
            if isinstance(frontmatter, dict):
                name = frontmatter.get("name")
                description = frontmatter.get("description")
            content = match.group(2)
        return cls(name=name, description=description, content=content)


@final
class SkillIO(ResourceIO[ExternalId, SkillRequest, SkillResponse]):
    folder_name = "skills"
    resource_cls = SkillResponse
    resource_write_cls = SkillRequest
    kind = "Skill"
    yaml_cls = SkillYAML
    _doc_base_url = ""
    _doc_url = "https://api-docs.cognite.com/20230101-beta/tag/Skills/operation/skill_create_ai_skills_post/"

    @classmethod
    def get_id(cls, item: SkillRequest | SkillResponse | dict) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=item.get("externalId", item["external_id"]))
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
    def _get_skill_sidecar_candidates(cls, filepath: Path, external_id: str) -> list[Path]:
        return [
            filepath.with_suffix(".md"),
            filepath.parent / f"{external_id}.{cls.kind}.md",
        ]

    @classmethod
    def get_extra_files(cls, filepath: Path, identifier: ExternalId, item: dict[str, Any]) -> Iterable[ReadExtra]:
        if "content" in item:
            return
        external_id = item.get("externalId") or item.get("external_id") or identifier.external_id or filepath.stem

        candidates = cls._get_skill_sidecar_candidates(filepath, str(external_id))
        skill_md_path = next((p for p in candidates if p.exists()), None)
        if skill_md_path is None:
            yield FailedReadExtra(
                source_path=filepath,
                code="MISSING",
                error=(
                    f"Missing skill content for {external_id!r} in {filepath.as_posix()}. "
                    f"No 'content' field found and no <prefix>.md file found. Expected one of: "
                    f"{humanize_collection(candidates)}"
                ),
            )
            return
        content = safe_read(skill_md_path, encoding=BUILD_FOLDER_ENCODING)
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
        for no, item in enumerate(raw_list):
            if "content" in item:
                continue

            external_id = item.get("externalId") or item.get("external_id") or filepath.stem
            candidates = self._get_skill_sidecar_candidates(filepath, str(external_id))
            skill_md_path = next((p for p in candidates if p.exists()), None)
            if skill_md_path is None:
                raise ToolkitFileNotFoundError(
                    f"'content' is missing and no <prefix>.Skill.md file found. Expected one of: "
                    f"{[str(p) for p in candidates]}",
                    filepath,
                )

            markdown = Markdown.from_markdown(safe_read(skill_md_path, encoding=BUILD_FOLDER_ENCODING))
            item = {
                **item,
                **markdown.model_dump(),
            }
            raw_list[no] = item

        return raw_list

    def dump_resource(self, resource: SkillResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_request_resource().dump()
        if local is None:
            return dumped
        if "content" not in local and dumped.get("content"):
            dumped.pop("content", None)
        return dumped

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
