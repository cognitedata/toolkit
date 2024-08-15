import inspect
import itertools
import re
import shutil

# from cognite.client.data_classes.transformations.common import TransformationDestination
from pathlib import Path
from typing import Any

import cognite.client.data_classes as data_classes
import yaml
from cognite.client.data_classes.iam import Capability
from cognite.client.data_classes.transformations import OidcCredentials
from cognite.client.utils._text import to_camel_case

from cognite_toolkit._cdf_tk.loaders import ResourceLoader
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml


# * in a line signifies that the line is a comment, for example where the config has multiple options
def comment_optional(yaml_str: str) -> str:
    n = []
    for line in yaml_str.split("\n"):
        if "*" in line:
            n.append(f"#{line}".replace("*", "").replace("'", "").replace(" - :", ""))
        else:
            n.append(line.replace("*", ""))
    return "\n".join(n)


IGNORED_ANNOTATIONS = [
    "str",
    "int",
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
    "list[ViewId | ViewApply]",
]


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


def expand_parameters(loader_cls: Any, resource_write_cls: Any) -> dict[str, Any]:
    init = resource_write_cls.__init__
    signature = inspect.signature(init)
    params: dict[str, Any] = {}

    for param in signature.parameters.values():
        if param.name == "self" or param.name == "cognite_client":
            continue

        str_annotations = str(param.annotation)
        str_annotations = str_annotations.replace("Sequence[", "list[")
        str_annotations = re.sub(r"Literal\[(.*?)\]", lambda match: match.group(1), str_annotations)

        if str_annotations == "dict[str, str] | None":
            str_annotations = "dict[str, str]"
            params[to_camel_case(param.name)] = {
                "*optional dict[str,str]": {"*key1": "value1", "*key2": "value2"},
            }
            continue

        if param.name == "data_set_id":
            params["dataSetExternalId"] = param.annotation.replace("int", "str")
            continue

        pattern = r"\|(?![^\[\]]*\])"
        annotations = [a.strip() for a in re.split(pattern, str_annotations)]

        if resource_write_cls.__name__ == "ExtractionPipelineWrite":
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

        elif resource_write_cls.__name__ == "FunctionWrite":
            if param.name == "runtime":
                params[param.name] = "py38 | py39 | py310 | py311 | None"
                continue

            if param.name == "file_id":
                continue

        elif resource_write_cls.__name__ == "FileMetadataWrite":
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

        elif resource_write_cls.__name__ == "TransformationWrite":
            if param.name == "source_nonce" or param.name == "destination_nonce":
                continue

            if param.name == "conflict_mode":
                params[to_camel_case(param.name)] = str_annotations
                continue

            if param.name == "source_oidc_credentials":
                params["authentication"] = {"*optional OidcCredentials": expand_parameters(loader_cls, OidcCredentials)}
                continue

            if param.name == "destination_oidc_credentials":
                continue

        elif resource_write_cls.__name__ == "TimeSeriesWrite":
            if param.name == "instance_id":
                params[to_camel_case(param.name)] = [{"space": "str", "external_id": "str"}, "None"]
                continue

        elif (
            resource_write_cls.__name__ == "WorkflowDefinitionUpsert"
            or resource_write_cls.__name__ == "WorkflowVersionUpsert"
        ):
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

        if not any([a not in IGNORED_ANNOTATIONS for a in annotations]):
            params[to_camel_case(param.name)] = str_annotations
            continue

        for annotation in annotations:
            expanded = []
            try:
                # groups
                if annotation == "list[Capability]":
                    expanded = expand_acls()
                else:
                    annotationCls = getattr(data_classes, annotation)
                    sub = expand_parameters(loader_cls, annotationCls)
                    expanded.append(sub)
            except Exception as e:
                if annotation in IGNORED_ANNOTATIONS:
                    expanded.append(annotation)
                    continue
                print(resource_write_cls.__name__)
                print(f"Failed to expand {param.name} {annotation}: {e}")
                raise e

            params[to_camel_case(param.name)] = expanded

    return params


def generate(target_path: Path) -> None:
    for loader in [
        loader for loader in ResourceLoader.__subclasses__() if loader.__name__ not in ["ResourceContainerLoader"]
    ]:
        write_cls = loader.resource_write_cls  # type: ignore

        ref = expand_parameters(loader, write_cls)

        final_yaml = comment_optional(yaml.dump(ref, sort_keys=False))

        # special case: want caps at the end
        if "capabilities" in ref:
            caps_yaml = comment_optional(yaml.dump({"capabilities": ref.pop("capabilities")}, sort_keys=False))
            main_yaml = comment_optional(yaml.dump(ref, sort_keys=False))
            final_yaml = f"{main_yaml}{caps_yaml}"

        folder = target_path / loader.folder_name
        file_name = folder / Path(f'reference.{write_cls.__name__.replace("Write","").replace("Apply","")}.yaml')
        Path.mkdir(folder, exist_ok=True)
        Path.write_text(file_name, final_yaml)

        print(f"Wrote {file_name}")


def validate(target_path: Path) -> None:
    total_warnings = 0
    for loader in [
        loader for loader in ResourceLoader.__subclasses__() if loader.__name__ not in ["ResourceContainerLoader"]
    ]:
        write_cls = loader.resource_write_cls  # type: ignore
        folder = target_path / loader.folder_name
        file_name = folder / Path(f'reference.{write_cls.__name__.replace("Write","").replace("Apply","")}.yaml')
        warnings = validate_resource_yaml(
            data=yaml.safe_load(Path.read_text(file_name)),
            spec=loader.get_write_cls_parameter_spec(),
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
    target_path = Path("cognite_toolkit/_cdf_tk/prototypes/_packages/reference")
    if Path.exists(target_path):
        shutil.rmtree(target_path)
    Path.mkdir(target_path)

    generate(target_path)
    validate(target_path)
