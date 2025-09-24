from __future__ import annotations

import abc
import collections.abc
import dataclasses
import enum
import inspect
import random
import re
import string
import typing
from collections.abc import Callable, Hashable
from datetime import date, datetime
from pathlib import Path
from types import UnionType
from typing import IO, Any, Literal, TypeVar, get_args, get_origin
from zoneinfo import ZoneInfo

import pytest
from _pytest.monkeypatch import MonkeyPatch
from cognite.client import CogniteClient
from cognite.client._constants import MAX_VALID_INTERNAL_ID
from cognite.client.data_classes import (
    Datapoints,
    DatapointsArray,
    DataPointSubscriptionWrite,
    EndTimeFilter,
    FunctionTaskOutput,
    FunctionTaskParameters,
    Geometry,
    Relationship,
    SequenceColumn,
    SequenceColumnList,
    SequenceData,
    SequenceRow,
    SequenceRows,
    Transformation,
    TransformationScheduleWrite,
    filters,
)
from cognite.client.data_classes._base import CogniteResourceList
from cognite.client.data_classes.capabilities import Capability, LegacyCapability, UnknownAcl
from cognite.client.data_classes.data_modeling import NodeListWithCursor
from cognite.client.data_classes.data_modeling.data_types import ListablePropertyType
from cognite.client.data_classes.data_modeling.query import NodeResultSetExpression, Query
from cognite.client.data_classes.filters import Filter
from cognite.client.data_classes.hosted_extractors import (
    BodyLoad,
    HeaderValueLoad,
    NextUrlLoad,
    QueryParamLoad,
    RestConfig,
)
from cognite.client.data_classes.transformations.notifications import TransformationNotificationWrite
from cognite.client.data_classes.workflows import (
    WorkflowTaskOutput,
    WorkflowTaskParameters,
    WorkflowTriggerDataModelingQuery,
)
from cognite.client.testing import CogniteClientMock
from cognite.client.utils.useful_types import SequenceNotStr
from questionary import Choice

from cognite_toolkit._cdf_tk._parameters.get_type_hints import _TypeHints
from cognite_toolkit._cdf_tk.client.data_classes.location_filters import (
    LocationFilter,
    LocationFilterScene,
)
from cognite_toolkit._cdf_tk.client.data_classes.sequences import ToolkitSequenceRows
from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.utils import load_yaml_inject_variables, read_yaml_file
from tests.data import COMPLETE_ORG

UNION_TYPES = {typing.Union, UnionType}


def mock_read_yaml_file(
    file_content_by_name: dict[str, dict | list], monkeypatch: MonkeyPatch, modify: bool = False
) -> None:
    def fake_read_yaml_file(
        filepath: Path, expected_output: Literal["list", "dict"] = "dict"
    ) -> dict[str, Any] | list[dict[str, Any]]:
        if file_content := file_content_by_name.get(filepath.name):
            if modify:
                source = read_yaml_file(filepath, expected_output)
                source.update(file_content)
                file_content = source
            return file_content
        return read_yaml_file(filepath, expected_output)

    def fake_load_yaml_inject_variables(
        filepath: Path | str,
        environment_variables: dict[str, str | None],
        required_return_type: Literal["any", "list", "dict"] = "any",
        validate: bool = True,
        original_filepath: Path | None = None,
    ) -> dict[str, Any] | list[dict[str, Any]]:
        if isinstance(filepath, str):
            return load_yaml_inject_variables(
                filepath, environment_variables, required_return_type, validate, original_filepath
            )
        if file_content := file_content_by_name.get(filepath.name):
            if modify:
                source = load_yaml_inject_variables(
                    filepath, environment_variables, required_return_type, validate, original_filepath
                )
                source.update(file_content)
                file_content = source
            return file_content
        return load_yaml_inject_variables(
            filepath, environment_variables, required_return_type, validate, original_filepath
        )

    monkeypatch.setattr("cognite_toolkit._cdf_tk.utils.read_yaml_file", fake_read_yaml_file)
    monkeypatch.setattr("cognite_toolkit._cdf_tk.data_classes._base.read_yaml_file", fake_read_yaml_file)
    monkeypatch.setattr("cognite_toolkit._cdf_tk.commands.deploy.read_yaml_file", fake_read_yaml_file)

    monkeypatch.setattr("cognite_toolkit._cdf_tk.utils.load_yaml_inject_variables", fake_load_yaml_inject_variables)
    monkeypatch.setattr(
        "cognite_toolkit._cdf_tk.cruds._base_cruds.load_yaml_inject_variables", fake_load_yaml_inject_variables
    )
    for module in [
        "classic",
        "datamodel",
        "industrial_tool",
        "transformation",
    ]:
        monkeypatch.setattr(
            f"cognite_toolkit._cdf_tk.cruds._resource_cruds.{module}.load_yaml_inject_variables",
            fake_load_yaml_inject_variables,
        )


class PrintCapture:
    # Find all text within square brackets
    _pattern = re.compile(r"\[([^]]+)\]")

    def __init__(self):
        self.messages = []

    def __call__(
        self,
        *objects: Any,
        sep: str = " ",
        end: str = "\n",
        file: IO[str] | None = None,
        flush: bool = False,
    ):
        for obj in objects:
            if isinstance(obj, str) and (clean := self._pattern.sub("", obj).strip()):
                # Remove square brackets and whitespace. This is to take
                # away the styling from rich print.
                self.messages.append(clean)


T_Type = TypeVar("T_Type", bound=type)


def all_subclasses(base: T_Type) -> list[T_Type]:
    """Returns a list (without duplicates) of all subclasses of a given class, sorted on import-path-name.
    Ignores classes not part of the main library, e.g. subclasses part of tests.
    """
    return sorted(
        filter(
            lambda sub: sub.__module__.startswith("cognite.client"),
            set(base.__subclasses__()).union(s for c in base.__subclasses__() for s in all_subclasses(c)),
        ),
        key=str,
    )


def all_concrete_subclasses(base: T_Type) -> list[T_Type]:
    return [
        sub
        for sub in all_subclasses(base)
        if all(base is not abc.ABC for base in sub.__bases__) and not inspect.isabstract(sub)
    ]


T_Object = TypeVar("T_Object", bound=object)


class FakeCogniteResourceGenerator:
    _error_msg: typing.ClassVar[str] = "Please extend this function to support generating fake data for this type"

    def __init__(
        self,
        seed: int | None = None,
        cognite_client: CogniteClientMock | CogniteClient | None = None,
        max_list_dict_items: int = 3,
    ) -> None:
        self._random = random.Random(seed)
        self._cognite_client = cognite_client or CogniteClientMock()
        self._max_list_dict_items = max_list_dict_items

    def create_instances(self, list_cls: type[T_Object], skip_defaulted_args: bool = False) -> T_Object:
        return list_cls(
            [self.create_instance(list_cls._RESOURCE, skip_defaulted_args) for _ in range(self._max_list_dict_items)]
        )

    def create_instance(self, resource_cls: type[T_Object], skip_defaulted_args: bool = False) -> T_Object:
        is_abstract = any(base is abc.ABC for base in resource_cls.__bases__)
        if is_abstract:
            subclasses = all_concrete_subclasses(resource_cls)
            if not subclasses:
                raise ValueError(f"Cannot create instance of abstract class {resource_cls.__name__}")
            resource_cls = self._random.choice(subclasses)

        signature = inspect.signature(resource_cls.__init__)
        type_hint_by_name = _TypeHints.get_type_hints_by_name(resource_cls)

        keyword_arguments: dict[str, Any] = {}
        positional_arguments: list[Any] = []
        for name, parameter in signature.parameters.items():
            if name == "self":
                continue
            elif name == "args" or name == "kwargs":
                # Skipping generic arguments.
                continue
            elif name.startswith("_"):
                continue
            elif parameter.annotation is inspect.Parameter.empty:
                raise ValueError(f"Parameter {name} of {resource_cls.__name__} is missing annotation")
            elif skip_defaulted_args and parameter.default is not inspect.Parameter.empty:
                continue

            if resource_cls is Geometry and name == "geometries":
                # Special case for Geometry to avoid recursion.
                value = None
            elif name == "scene":
                value = self.create_value(LocationFilterScene, var_name=name)
            elif name == "version":
                # Special case
                value = random.choice(["v1", "v2", "v3"])
            elif resource_cls is LocationFilter and name == "locations":
                # Special case for LocationFilter to avoid recursion.
                value = None
            else:
                value = self.create_value(type_hint_by_name[name], var_name=name)

            if parameter.kind in {parameter.POSITIONAL_ONLY, parameter.VAR_POSITIONAL}:
                positional_arguments.append(value)
            else:
                keyword_arguments[name] = value

        # Special cases
        if resource_cls is DataPointSubscriptionWrite:
            # DataPointSubscriptionWrite requires either timeseries_ids or filter
            if skip_defaulted_args:
                keyword_arguments["time_series_ids"] = ["my_timeseries1", "my_timeseries2"]
            else:
                keyword_arguments.pop("filter", None)

        if resource_cls is Query or resource_cls is WorkflowTriggerDataModelingQuery:
            # The fake generator makes all dicts from 1-3 values, we need to make sure that the query is valid
            # by making sure that the list of equal length, so we make both to length 1.
            with_key, with_value = next(iter(keyword_arguments["with_"].items()))
            select_value = next(iter(keyword_arguments["select"].values()))
            keyword_arguments["with_"] = {with_key: with_value}
            keyword_arguments["select"] = {with_key: select_value}
        elif resource_cls is Relationship and not skip_defaulted_args:
            # Relationship must set the source and target type consistently with the source and target
            keyword_arguments["source_type"] = type(keyword_arguments["source"]).__name__
            keyword_arguments["target_type"] = type(keyword_arguments["target"]).__name__
        elif resource_cls is Datapoints and not skip_defaulted_args:
            # All lists have to be equal in length and only value and timestamp
            keyword_arguments["timestamp"] = keyword_arguments["timestamp"][:1]
            keyword_arguments["value"] = keyword_arguments["value"][:1]
            for key in list(keyword_arguments):
                if isinstance(keyword_arguments[key], list) and key not in {"timestamp", "value"}:
                    keyword_arguments.pop(key)
        elif resource_cls is DatapointsArray:
            keyword_arguments["is_string"] = False
        elif resource_cls is SequenceRows or resource_cls is ToolkitSequenceRows:
            # All row values must match the number of columns
            # Reducing to one column, and one value for each row
            if skip_defaulted_args:
                keyword_arguments["external_id"] = "my_sequence_rows"
            keyword_arguments["columns"] = keyword_arguments["columns"][:1]
            for row in keyword_arguments["rows"]:
                row.values = row.values[:1]
        elif resource_cls is SequenceData:
            if skip_defaulted_args:
                # At least external_id or id must be set
                keyword_arguments["external_id"] = "my_sequence"
                keyword_arguments["rows"] = [
                    SequenceRow(
                        row_number=1,
                        values=[
                            1,
                        ],
                    )
                ]
                keyword_arguments["columns"] = SequenceColumnList(
                    [
                        SequenceColumn("my_column"),
                    ]
                )
            else:
                # All row values must match the number of columns
                keyword_arguments.pop("rows", None)
                keyword_arguments["columns"] = keyword_arguments["columns"][:1]
                keyword_arguments["row_numbers"] = keyword_arguments["row_numbers"][:1]
                keyword_arguments["values"] = keyword_arguments["values"][:1]
                keyword_arguments["values"][0] = keyword_arguments["values"][0][:1]
        elif resource_cls is EndTimeFilter:
            # EndTimeFilter requires either is null or (max and/or min)
            keyword_arguments.pop("is_null", None)
        elif resource_cls is Transformation and not skip_defaulted_args:
            # schedule and jobs must match external id and id
            keyword_arguments["schedule"].external_id = keyword_arguments["external_id"]
            keyword_arguments["schedule"].id = keyword_arguments["id"]
            keyword_arguments["running_job"].transformation_id = keyword_arguments["id"]
            keyword_arguments["last_finished_job"].transformation_id = keyword_arguments["id"]
        elif resource_cls is TransformationScheduleWrite:
            # TransformationScheduleWrite requires either id or external_id
            keyword_arguments.pop("id", None)
        elif resource_cls is TransformationNotificationWrite:
            # TransformationNotificationWrite requires either transformation_id or transformation_external_id
            if skip_defaulted_args:
                keyword_arguments["transformation_external_id"] = "my_transformation"
            else:
                keyword_arguments.pop("transformation_id", None)
        elif resource_cls is NodeResultSetExpression and not skip_defaulted_args:
            # Through has a special format.
            keyword_arguments["through"] = [keyword_arguments["through"][0], "my_view/v1", "a_property"]
        elif resource_cls is RestConfig:
            incremental = keyword_arguments.get("incremental_load")
            if isinstance(incremental, NextUrlLoad):
                # The incremental load cannot be of type `nextUrl`
                load_cls = self._random.choice([BodyLoad, HeaderValueLoad, QueryParamLoad])
                keyword_arguments["incremental_load"] = self.create_instance(load_cls, skip_defaulted_args)
        elif issubclass(resource_cls, ListablePropertyType) and "max_list_size" in keyword_arguments:
            if keyword_arguments["max_list_size"] <= 1:
                keyword_arguments.pop("max_list_size")
            elif keyword_arguments["max_list_size"] > 1:
                keyword_arguments["max_list_size"] = min(2_000, keyword_arguments["max_list_size"])
                keyword_arguments["is_list"] = True
        elif resource_cls is ZoneInfo:
            # ZoneInfo does not allow setting any parameters
            return ZoneInfo("UTC")
        return resource_cls(*positional_arguments, **keyword_arguments)

    def create_value(self, type_: Any, var_name: str | None = None) -> Any:
        import numpy as np

        if isinstance(type_, typing.ForwardRef):
            type_ = type_._evaluate(globals(), self._type_checking())

        container_type = get_origin(type_)
        is_container = container_type is not None
        if not is_container or container_type is np.ndarray:  # looks weird, but 3.8 and 3.12 type compat. issue
            # Handle numpy types
            from numpy.typing import NDArray

            if type_ == NDArray[np.float64]:
                return np.array([self._random.random() for _ in range(3)], dtype=np.float64)
            elif type_ == NDArray[np.uint32]:
                return np.array([self._random.randint(1, 100) for _ in range(3)], dtype=np.uint32)
            elif type_ == NDArray[np.int64]:
                return np.array([self._random.randint(1, 100) for _ in range(3)], dtype=np.int64)
            elif type_ == NDArray[np.datetime64]:
                return np.array([self._random.randint(1, 1704067200000) for _ in range(3)], dtype="datetime64[ns]")
            elif type_ == NDArray[np.object_]:
                return np.array([self._random_string(10) for _ in range(3)], dtype=np.object_)

        # Handle containers
        args = get_args(type_)
        first_not_none = next((arg for arg in args if arg is not None), None)
        if get_origin(first_not_none) is Callable and type(None) in args:
            # If the type is Optional[Callable], we cannot generate a value, so we return None
            return None

        if container_type in UNION_TYPES:
            return self.create_value(first_not_none)
        elif container_type is typing.Literal:
            return self._random.choice(args)
        elif container_type in [
            list,
            typing.Sequence,
            collections.abc.Sequence,
            collections.abc.Collection,
            SequenceNotStr,
        ]:
            return [self.create_value(first_not_none) for _ in range(3)]
        elif container_type in [dict, collections.abc.MutableMapping, collections.abc.Mapping]:
            if first_not_none is None:
                return self.create_value(dict)
            key_type, value_type = args
            return {
                self.create_value(key_type): self.create_value(value_type) for _ in range(self._random.randint(1, 3))
            }
        elif container_type is set:
            return set(self.create_value(first_not_none) for _ in range(self._random.randint(1, 3)))
        elif container_type is tuple:
            if any(arg is ... for arg in args):
                return tuple(self.create_value(first_not_none) for _ in range(self._random.randint(1, 3)))
            return tuple(self.create_value(arg) for arg in args)
        elif container_type is NodeListWithCursor:
            return NodeListWithCursor(
                [self.create_value(first_not_none) for _ in range(self._random.randint(1, 3))],
                cursor=self.create_value(str),
            )

        if var_name == "external_id" and type_ is str:
            return self._random_string(50, sample_from=string.ascii_uppercase + string.digits)
        elif var_name == "id" and type_ is int:
            return self._random.choice(range(1, MAX_VALID_INTERNAL_ID + 1))
        if type_ is str or type_ is Any:
            return self._random_string()
        elif type_ is int:
            return self._random.randint(1, 100000)
        elif type_ is float:
            return self._random.random()
        elif type_ is bool:
            return self._random.choice([True, False])
        elif type_ == datetime:
            return datetime.fromtimestamp(self._random.randint(1, 1704067200))
        elif type_ == date:
            return date.fromtimestamp(self._random.randint(1, 1704067200))
        elif type_ is dict:
            return {self._random_string(10): self._random_string(10) for _ in range(self._random.randint(1, 3))}
        elif type_ is CogniteClient:
            return self._cognite_client
        elif inspect.isclass(type_) and any(base is abc.ABC for base in type_.__bases__):
            implementations = all_concrete_subclasses(type_)
            if type_ is Filter:
                # Remove filters not supported by dps subscriptions
                implementations.remove(filters.Overlaps)

                # Remove filters which are only used by data modeling classes
                implementations.remove(filters.HasData)
                implementations.remove(filters.InvalidFilter)
                implementations.remove(filters.Nested)
                implementations.remove(filters.GeoJSONWithin)
                implementations.remove(filters.GeoJSONDisjoint)
                implementations.remove(filters.GeoJSONIntersects)
            elif type_ is Capability:
                implementations.remove(UnknownAcl)
                if LegacyCapability in implementations:
                    implementations.remove(LegacyCapability)
            if type_ is WorkflowTaskOutput:
                # For Workflow Output has to match the input type
                selected = FunctionTaskOutput
            elif type_ is WorkflowTaskParameters:
                selected = FunctionTaskParameters
            else:
                selected = self._random.choice(implementations)
            return self.create_instance(selected)
        elif isinstance(type_, enum.EnumMeta):
            return self._random.choice(list(type_))
        elif isinstance(type_, TypeVar):
            return self.create_value(type_.__bound__)
        elif inspect.isclass(type_) and issubclass(type_, CogniteResourceList):
            return type_([self.create_value(type_._RESOURCE) for _ in range(self._random.randint(1, 3))])
        elif type_ is Hashable:
            return "my_hashable"
        elif inspect.isclass(type_):
            return self.create_instance(type_)
        elif type(type_) is dataclasses.InitVar:
            return self.create_value(type_.type)

        raise NotImplementedError(f"Unsupported {type_=} or {container_type=}. {self._error_msg}")

    def _random_string(
        self,
        size: int | None = None,
        sample_from: str = string.ascii_uppercase + string.digits + string.ascii_lowercase + string.punctuation,
    ) -> str:
        k = size or self._random.randint(1, 100)
        return "".join(self._random.choices(sample_from, k=k))

    @classmethod
    def _type_checking(cls) -> dict[str, Any]:
        """
        When calling the get_type_hints function, it imports the module with the function TYPE_CHECKING is set to False.

        This function takes all the special types used in data classes and returns them as a dictionary so it
        can be used in the local namespaces.
        """
        import numpy as np
        import numpy.typing as npt
        from cognite.client import CogniteClient

        NumpyDatetime64NSArray = npt.NDArray[np.datetime64]
        NumpyInt64Array = npt.NDArray[np.int64]
        NumpyFloat64Array = npt.NDArray[np.float64]
        NumpyObjArray = npt.NDArray[np.object_]
        return {
            "CogniteClient": CogniteClient,
            "NumpyDatetime64NSArray": NumpyDatetime64NSArray,
            "NumpyInt64Array": NumpyInt64Array,
            "NumpyFloat64Array": NumpyFloat64Array,
            "NumpyObjArray": NumpyObjArray,
        }


class MockQuestion:
    def __init__(self, answer: Any, choices: list[Choice] | None = None) -> None:
        self.answer = answer
        self.choices = choices

    def ask(self) -> Any:
        if isinstance(self.answer, Callable):
            return self.answer(self.choices)
        return self.answer


class MockQuestionary:
    def __init__(self, module_target: str, monkeypatch: MonkeyPatch, answers: list[Any]) -> None:
        self.module_target = module_target
        self.answers = answers
        self.monkeypatch = monkeypatch

    def select(self, _: str, choices: list[Choice] | None = None, **__) -> MockQuestion:
        return MockQuestion(self.answers.pop(0), choices)

    def confirm(self, *_, **__) -> MockQuestion:
        return MockQuestion(self.answers.pop(0))

    def checkbox(self, *_, choices: list[Choice], **__) -> MockQuestion:
        return MockQuestion(self.answers.pop(0), choices)

    def text(self, *_, **__) -> MockQuestion:
        return MockQuestion(self.answers.pop(0))

    def __enter__(self):
        for method in [self.select, self.confirm, self.checkbox, self.text]:
            self.monkeypatch.setattr(f"{self.module_target}.questionary.{method.__name__}", method)
        return self

    def __exit__(self, *args):
        self.monkeypatch.undo()
        return False

    @staticmethod
    def select_all(choices: list[Choice]) -> list[str]:
        if not choices:
            return []
        return [choice.value for choice in choices]


def find_resources(resource: str, resource_dir: str | None = None, base: Path = COMPLETE_ORG / MODULES):
    for path in base.rglob(f"*{resource}.yaml"):
        if resource_dir and resource_dir not in path.parts:
            continue
        data = read_yaml_file(path)
        if isinstance(data, dict):
            yield pytest.param(data, id=path.relative_to(base).as_posix())
        elif isinstance(data, list):
            for no, item in enumerate(data):
                if isinstance(item, dict):
                    yield pytest.param(item, id=f"{path.relative_to(base).as_posix()} - Item: {no}")
                else:
                    raise ValueError(f"Invalid data format in {path}: {item}")
        else:
            raise ValueError(f"Invalid data format in {path}: {data}")
