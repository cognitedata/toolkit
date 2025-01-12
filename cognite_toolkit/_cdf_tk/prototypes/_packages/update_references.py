from __future__ import annotations

import inspect
import itertools
import re
import shutil
from pathlib import Path
from typing import Any

import cognite.client.data_classes as data_classes
import cognite.client.data_classes.data_modeling as dm_data_classes
import yaml
from cognite.client.data_classes.data_modeling import ViewApply, ViewId
from cognite.client.data_classes.iam import Capability
from cognite.client.data_classes.transformations import OidcCredentials
from cognite.client.data_classes.transformations.common import TransformationDestination
from cognite.client.utils._text import to_camel_case

from cognite_toolkit._cdf_tk.loaders import ResourceLoader
from cognite_toolkit._cdf_tk.utils import safe_read
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml


# * in a line signifies that the line is a comment, for example, where the config has multiple options
def comment_optional(yaml_str: str) -> str:
    n = []
    for line in yaml_str.split("\n"):
        if "*" in line:
            n.append(f"#{line}".replace("*", "").replace("'", "").replace(" - :", ""))
        else:
            n.append(line.replace("*", ""))
    return "\n".join(n)


IGNORED_ANNOTATIONS = {
    "str",
    # "int",
    "float",
    "bool",
    "dict",
    "list",
    "None",
    "enum",
    "dict[str, str]",
    "list[str]",
    "Any",
    "Filter",
    "list[ExternalId]",
    "'allUserAccounts'",
    "list[ViewId]",
    "dict[str, ViewPropertyApply]",
}


def expand_acls() -> list[Any]:
    expanded = []
    for acl in itertools.chain(Capability.__subclasses__()):
        if acl.__name__ == "LegacyCapability" or acl.__name__ == "UnknownAcl" or acl.__name__ == "SeismicAcl":
            continue
        action_members = []
        for name, value in acl.Action.__dict__.items():
            if not name.startswith("_") and not callable(value):
                action_members.append(value.value)

        scope_members: dict[str, str | dict] = {}
        filtered_classes = {name: cls for name, cls in acl.Scope.__dict__.items() if not name.startswith("_")}
        for name, cls in filtered_classes.items():
            if cls._scope_name == "all":
                scope_members["all"] = {}
            else:
                if "ids" in cls.__annotations__:
                    scope_members[f"*{cls._scope_name}"] = {"*ids": "list[str]"}
                else:
                    scope_members[f"{cls._scope_name}"] = {}

        expanded.append(
            {
                acl._capability_name: {
                    "actions": action_members,
                    "scope": scope_members
                    if len(scope_members) == 1
                    else {"*one of": {"*" + k: v for k, v in scope_members.items()}},
                }
            }
        )
    return expanded


def expand_transformation_destinations() -> dict[str, Any]:
    options: dict[str, Any] = {}
    options["*type"] = []
    for name, member in inspect.getmembers(TransformationDestination):
        if isinstance(TransformationDestination.__dict__.get(name), staticmethod):
            signature = inspect.signature(member)
            if len(signature.parameters) == 0:
                options["*type"].append(f'*"{name}"')
            else:
                options[f"*{to_camel_case(name)}"] = {k: f"*{v.annotation}" for k, v in signature.parameters.items()}
    return {"*one of": options}


def get_parameters(cls: Any) -> Any:
    sign = inspect.signature(cls.__init__)

    filtered_params = [param for name, param in sign.parameters.items() if name != "self" and name != "cognite_client"]
    return sign.replace(parameters=filtered_params).parameters.values()


def unpack_annotations(str_annotations: str) -> Any:
    str_annotations = str_annotations.replace("Sequence[", "list[")
    str_annotations = re.sub(r"Literal\[(.*?)\]", lambda match: match.group(1), str_annotations)

    pattern = r"\|(?![^\[\]]*\])"
    annotations = [a.strip() for a in re.split(pattern, str_annotations)]

    if not any([a not in IGNORED_ANNOTATIONS for a in annotations]):
        return str_annotations

    cognite_classes = {name: obj for name, obj in inspect.getmembers(data_classes) if inspect.isclass(obj)}
    cognite_classes.update({name: obj for name, obj in inspect.getmembers(dm_data_classes) if inspect.isclass(obj)})

    resolved: dict[str, Any] = {}

    for annotation in annotations:
        if annotation.startswith("list["):
            return [unpack_annotations(re.sub(r"list\[(.*?)\]", lambda match: match.group(1), annotation))]
        else:
            cls = cognite_classes.get(annotation)
            if not cls:
                continue
            for param in get_parameters(cls):
                resolved[to_camel_case(param.name)] = unpack_annotations(str(param.annotation))

        return resolved

    return str_annotations


def expand_parameters(loader_cls: Any, cls: Any, optional: bool = False) -> dict[str, Any]:
    parameters = get_parameters(cls)
    params: dict[str, Any] = {}

    for param in parameters:
        # resource specifics:
        if cls.__name__ == "ExtractionPipelineWrite":
            if param.name == "raw_tables":
                params[to_camel_case(param.name)] = {
                    "*optional list[dict[str, str]]:": [
                        {"*db1": "*table1"},
                        {"*db2": "*table2"},
                        {"*db3": {}},
                    ]
                }
                continue

            if param.name == "contacts":
                params[param.name] = {
                    "*optional list[ExtractionPipelineContact] ": [
                        {"*name": "str", "*email": "*str", "*role": "str", "*sendNotification": "True | False"},
                        {"*name": "str", "*email": "*str", "*role": "str", "*sendNotification": "True | False"},
                    ]
                }
                continue

        elif cls.__name__ == "FunctionWrite":
            if param.name == "runtime":
                params[param.name] = "py38 | py39 | py310 | py311 | None"
                continue

            if param.name == "file_id":
                continue

        elif cls.__name__ == "FileMetadataWrite":
            if param.name == "labels":
                params[param.name] = {"*optional list[Label]": ["*label_1_ext_id", "*label_2_ext_id"]}
                continue

            if param.name == "geo_location":
                params[to_camel_case(param.name)] = {
                    "*optional GeoLocation": {
                        "*type": "'feature'",
                        "*geometry": "Point | MultiPoint | LineString | MultiLineString | Polygon | MultiPolygon",
                        "*properties": "dict | None",
                    }
                }
                continue

            if param.name == "asset_ids":
                params[to_camel_case(param.name)] = {"*optional list[int]": ["*asset_1_ext_id", "*asset_2_ext_id"]}
                continue

            if param.name == "security_categories":
                params[to_camel_case(param.name)] = {
                    "*optional list[SecurityCategory]": ["*sec_cat_1_ext_id", "*sec_cat_2_ext_id"]
                }
                continue

        elif cls.__name__ == "TransformationWrite":
            if param.name == "source_nonce" or param.name == "destination_nonce":
                continue

            if param.name == "conflict_mode":
                params[to_camel_case(param.name)] = {
                    "*optional": {"*one of": ['*"abort"', '*"*delete"', '*"update"', '*"upsert"']}
                }
                continue

            if param.name == "source_oidc_credentials":
                params["authentication"] = {
                    "*optional OidcCredentials": expand_parameters(loader_cls, OidcCredentials, optional=True)
                }
                continue

            if param.name == "destination_oidc_credentials":
                continue

            if param.name == "destination":
                params["destination"] = expand_transformation_destinations()
                continue

        elif cls.__name__ == "TimeSeriesWrite":
            if param.name == "instance_id":
                params[to_camel_case(param.name)] = [{"space": "str", "external_id": "str"}, "None"]
                continue

        elif cls.__name__ == "WorkflowDefinitionUpsert" or cls.__name__ == "WorkflowVersionUpsert":
            if param.name == "workflow_definition":
                params[to_camel_case(param.name)] = {
                    "description": "str | None",
                    "tasks": [
                        {
                            "externalId": "str",
                            "type": "function | transformation | cdf",
                            "parameters": {
                                "function": {
                                    "externalId": "str",
                                }
                            },
                        }
                    ],
                }
                continue

        elif cls.__name__ == "GroupWrite":
            if param.name == "capabilities":
                params[to_camel_case(param.name)] = expand_acls()
                continue

        elif cls.__name__ == "DataModelApply":
            if param.name == "views":
                params[to_camel_case(param.name)] = {
                    "*optional list[ViewId | ViewApply]": {
                        "*one list of": [expand_parameters(cls, ViewId, True), expand_parameters(cls, ViewApply, True)]
                    }
                }
                continue

        if str(param.annotation) == "dict[str, str] | None":
            params[to_camel_case(param.name)] = {
                "*optional dict[str,str]": {"*key1": "value1", "*key2": "value2"},
            }
            continue

        if param.name == "data_set_id":
            params["dataSetExternalId"] = param.annotation.replace("int", "str")
            continue

        params[to_camel_case(param.name)] = f"{'*' if optional else ''}{unpack_annotations(str(param.annotation))}"

    return params


def generate(target_path: Path) -> None:
    for loader_cls in [
        loader for loader in ResourceLoader.__subclasses__() if loader.__name__ not in ["ResourceContainerLoader"]
    ]:
        write_cls = loader_cls.resource_write_cls  # type: ignore

        ref = expand_parameters(loader_cls, write_cls)

        final_yaml = comment_optional(yaml.dump(ref, sort_keys=False))

        # special case for groups: want caps at the end
        if "capabilities" in ref:
            caps_yaml = comment_optional(yaml.dump({"capabilities": ref.pop("capabilities")}, sort_keys=False))
            main_yaml = comment_optional(yaml.dump(ref, sort_keys=False))
            final_yaml = f"{main_yaml}{caps_yaml}"

        folder = target_path / loader_cls.folder_name
        file_name = folder / Path(f"reference.{loader_cls.kind}.yaml")
        Path.mkdir(folder, exist_ok=True)
        Path.write_text(file_name, final_yaml)

        print(f"Wrote {file_name}")

        print(f"Wrote {file_name}")


def validate(target_path: Path) -> None:
    total_warnings = 0
    for loader_cls in [
        loader for loader in ResourceLoader.__subclasses__() if loader.__name__ not in ["ResourceContainerLoader"]
    ]:
        folder = target_path / loader_cls.folder_name
        file_name = folder / Path(f"reference.{loader_cls.kind}.yaml")
        warnings = validate_resource_yaml(
            data=yaml.safe_load(safe_read(file_name)),
            spec=loader_cls.get_write_cls_parameter_spec(),
            source_file=file_name,
        )

        total_warnings += len(warnings)
        if len(warnings) > 0:
            print(f"Warnings for {file_name}")
            for warning in warnings:
                print(warning)
            break

    print(f"Total warnings: {total_warnings}")


if __name__ == "__main__":
    target_path = Path("cognite_toolkit/_cdf_tk/prototypes/_packages/reference/references")
    if Path.exists(target_path):
        shutil.rmtree(target_path)
    Path.mkdir(target_path)

    generate(target_path)
    validate(target_path)
    manifest_file = target_path.parent / Path("manifest.yaml")
    manifest_content = {
        "title": "References: reference yaml files for the supported resources",
        "modules": {
            "references": {
                "title": "Reference files",
            },
        },
    }
    manifest_file.write_text(yaml.dump(manifest_content, sort_keys=False))
