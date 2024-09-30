from __future__ import annotations

import itertools
from datetime import datetime
from typing import Any, Literal

from cognite.client import CogniteClient
from cognite.client.data_classes._base import CogniteResourceList, WriteableCogniteResourceList
from cognite.client.data_classes.data_modeling import DirectRelationReference, ViewId
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteFile, CogniteFileApply
from cognite.client.utils._text import to_camel_case


class ExtendableCogniteFileApply(CogniteFileApply):
    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        source_id: str | None = None,
        source_context: str | None = None,
        source: DirectRelationReference | tuple[str, str] | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        assets: list[DirectRelationReference | tuple[str, str]] | None = None,
        mime_type: str | None = None,
        directory: str | None = None,
        category: DirectRelationReference | tuple[str, str] | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
        node_source: ViewId | None = None,
        extra_properties: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(
            space=space,
            external_id=external_id,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            source_id=source_id,
            source_context=source_context,
            source=source,
            source_created_time=source_created_time,
            source_updated_time=source_updated_time,
            source_created_user=source_created_user,
            source_updated_user=source_updated_user,
            assets=assets,
            mime_type=mime_type,
            directory=directory,
            category=category,
            existing_version=existing_version,
            type=type,
        )
        self.node_source = node_source
        self.extra_properties = extra_properties

    def dump(self, camel_case: bool = True, context: Literal["api", "local"] = "api") -> dict[str, Any]:
        """Dumps the object to a dictionary.

        Args:
            camel_case: Whether to use camel case or not.
            context: If 'api', the output is for the API and will match the Node API schema. If 'local', the output is
                for a YAML file and all properties are  on the same level as the node properties. See below

        Example:
            >>> node = ExtendableCogniteFileApply(space="space", external_id="external_id", name="name")
            >>> node.dump(camel_case=True, context="api")
            {
                "space": "space",
                "externalId": "external_id",
                "sources": [
                    {
                        "source": {
                            "space": "cdf_cdm",
                            "externalId": "CogniteFile",
                            "version": "v1",
                            "type": "view"
                        }
                        "properties": {
                            "name": "name"
                        }
                    }
                ]
            }
            >>> node.dump(camel_case=True, context="local")
            {
                "space": "space",
                "external_id": "external_id",
                "name": "name",
            }

        Returns:

        """
        output = super().dump(camel_case)
        source = output["sources"][0]
        source["properties"].pop("node_source", None)
        source["properties"].pop("extra_properties", None)
        if context == "api":
            if self.node_source is not None:
                source["source"] = self.node_source.dump(include_type=True)
            if self.extra_properties is not None:
                source["properties"].update(self.extra_properties)
        else:
            output.pop("sources", None)
            output.update(source["properties"])
            if self.node_source is not None:
                output["nodeSource" if camel_case else "node_source"] = self.node_source.dump(include_type=False)
            if self.extra_properties is not None:
                output.update(self.extra_properties)
        return output

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> ExtendableCogniteFileApply:
        base_props = cls._load_base_properties(resource)
        properties = cls._load_properties(resource)
        loaded_keys = {to_camel_case(p) for p in itertools.chain(base_props.keys(), properties.keys())} | {
            "instanceType",
            "isUploaded",
            "uploadedTime",
        }
        if "nodeSource" in resource:
            properties["node_source"] = ViewId.load(resource["nodeSource"])
            loaded_keys.add("nodeSource")
        if extra_keys := (set(resource) - loaded_keys):
            properties["extra_properties"] = {key: resource[key] for key in extra_keys}

        return cls(**base_props, **properties)


class ExtendableCogniteFile(CogniteFile):
    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        source_id: str | None = None,
        source_context: str | None = None,
        source: DirectRelationReference | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        assets: list[DirectRelationReference] | None = None,
        mime_type: str | None = None,
        directory: str | None = None,
        is_uploaded: bool | None = None,
        uploaded_time: datetime | None = None,
        category: DirectRelationReference | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
        extra_properties: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(
            space=space,
            external_id=external_id,
            version=version,
            last_updated_time=last_updated_time,
            created_time=created_time,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            source_id=source_id,
            source_context=source_context,
            source=source,
            source_created_time=source_created_time,
            source_updated_time=source_updated_time,
            source_created_user=source_created_user,
            source_updated_user=source_updated_user,
            assets=assets,
            mime_type=mime_type,
            directory=directory,
            is_uploaded=is_uploaded,
            uploaded_time=uploaded_time,
            category=category,
            type=type,
            deleted_time=deleted_time,
        )
        self.extra_properties = extra_properties

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId(space="cdf_cdm", external_id="CogniteFile", version="v1")

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> ExtendableCogniteFile:
        base_props = cls._load_base_properties(resource)
        all_properties = resource.get("properties", {})
        # There should only be one source in one view
        if all_properties:
            view_props = next(iter(all_properties.values()))
            node_props = next(iter(view_props.values()))
            properties = cls._load_properties(node_props)
        else:
            properties = {}
        return cls(**base_props, **properties)

    def as_write(self) -> ExtendableCogniteFileApply:
        return ExtendableCogniteFileApply(
            space=self.space,
            external_id=self.external_id,
            name=self.name,
            description=self.description,
            tags=self.tags,
            aliases=self.aliases,
            source_id=self.source_id,
            source_context=self.source_context,
            source=self.source,
            source_created_time=self.source_created_time,
            source_updated_time=self.source_updated_time,
            source_created_user=self.source_created_user,
            source_updated_user=self.source_updated_user,
            assets=self.assets,  # type: ignore[arg-type]
            mime_type=self.mime_type,
            directory=self.directory,
            category=self.category,
            existing_version=self.version,
            type=self.type,
            node_source=None,
            extra_properties=self.extra_properties,
        )


class ExtendableCogniteFileApplyList(CogniteResourceList[ExtendableCogniteFileApply]):
    _RESOURCE = ExtendableCogniteFileApply


class ExtendableCogniteFileList(WriteableCogniteResourceList[ExtendableCogniteFileApply, ExtendableCogniteFile]):
    _RESOURCE = ExtendableCogniteFile

    def as_write(self) -> ExtendableCogniteFileApplyList:
        return ExtendableCogniteFileApplyList([model.as_write() for model in self.data])
