from collections.abc import Iterable
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import ViewId
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildVariable
from cognite_toolkit._cdf_tk.commands.pull import PullV2Command
from cognite_toolkit._cdf_tk.resource_ios import DataSetsIO, ViewIO
from tests.test_unit.approval_client import ApprovalToolkitClient


def _built_resource(identifier: Identifier, variables: list[BuildVariable] | None = None) -> MagicMock:
    resource = MagicMock()
    resource.identifier = identifier
    resource.variables = variables or []
    resource.extra_files = []
    return resource


def to_write_content_use_cases() -> Iterable:
    source = """name: Ingestion
externalId: {{ dataset }}
description: This dataset contains Transformations, Functions, and Workflows for ingesting data into Cognite Data Fusion.
"""
    to_write = {
        ExternalId(external_id="ingestion"): {
            "name": "Ingestion",
            "externalId": "ingestion",
            "description": "New description",
        }
    }
    variable = BuildVariable(
        id=Path("modules/dataset"),
        value="ingestion",
        is_selected=True,
    )
    ingestion = _built_resource(ExternalId(external_id="ingestion"), [variable])
    resources = [ingestion]

    expected = """name: Ingestion
externalId: {{ dataset }}
description: New description
"""

    yield pytest.param(
        source, to_write, resources, expected, DataSetsIO, Path("my.DataSet.yaml"), id="One resource changed"
    )

    source = """name: Ingestion
externalId: {{ dataset }} # This is a comment
# This is another comment
description: Original description
"""

    expected = """name: Ingestion
externalId: {{ dataset }} # This is a comment
# This is another comment
description: New description
"""

    yield pytest.param(
        source,
        to_write,
        resources,
        expected,
        DataSetsIO,
        Path("my.DataSet.yaml"),
        id="One resource changed with comments",
    )

    source = """- name: Ingestion
  externalId: {{ dataset }} # This is a comment
  # This is another comment
  description: Original description
- name: Another
  externalId: unique_dataset
  description: with its own description
"""

    expected = """- name: Ingestion
  externalId: {{ dataset }} # This is a comment
  # This is another comment
  description: New description
- name: Another
  externalId: unique_dataset
  description: also new description
"""
    to_write_multi = {
        **to_write,
        ExternalId(external_id="unique_dataset"): {
            "name": "Another",
            "externalId": "unique_dataset",
            "description": "also new description",
        },
    }
    unique_dataset = _built_resource(ExternalId(external_id="unique_dataset"))
    resources = [ingestion, unique_dataset]

    yield pytest.param(
        source,
        to_write_multi,
        resources,
        expected,
        DataSetsIO,
        Path("my.DataSet.yaml"),
        id="Multiple resources changed",
    )

    source = """space: {{ instance_space }}
externalId: my_external_id
version: v1
filter:
  hasData:
  - type: ContainerReference
    space: data_space
    externalId: data_external_id
  - type: ContainerReference
    space: another_data_space
    externalId: another_data_external_id
"""
    to_write_view = {
        ViewId(space="my_space", external_id="my_external_id", version="v1"): {
            "space": "my_space",
            "externalId": "my_external_id",
            "version": "v1",
            "filter": {
                "hasData": [
                    {
                        "type": "ContainerReference",
                        "space": "data_space",
                        "externalId": "data_external_id",
                    },
                    {
                        "type": "ContainerReference",
                        "space": "another_data_space",
                        "externalId": "another_data_external_id",
                    },
                ]
            },
        }
    }
    variable_view = BuildVariable(
        id=Path("modules/instance_space"),
        value="my_space",
        is_selected=True,
    )
    view_resource = _built_resource(
        ViewId(space="my_space", external_id="my_external_id", version="v1"),
        [variable_view],
    )
    resources_view = [view_resource]

    expected_view = """space: {{ instance_space }}
externalId: my_external_id
version: v1
filter:
  hasData:
  - type: ContainerReference
    space: data_space
    externalId: data_external_id
  - type: ContainerReference
    space: another_data_space
    externalId: another_data_external_id
"""

    yield pytest.param(
        source,
        to_write_view,
        resources_view,
        expected_view,
        ViewIO,
        Path("my.View.yaml"),
        id="View with filter and no differences",
    )


class TestPullV2Command:
    @pytest.mark.parametrize(
        "source, to_write, resources, expected, loader_type, source_file",
        list(to_write_content_use_cases()),
    )
    def test_to_write_content(
        self,
        source: str,
        to_write: dict[Identifier, dict[str, Any]],
        resources: list[MagicMock],
        expected: str,
        loader_type: type,
        source_file: Path,
        toolkit_client_approval: ApprovalToolkitClient,
    ) -> None:
        cmd = PullV2Command(silent=True, skip_tracking=True)

        actual, extra_files = cmd._to_write_content(
            source_content=source,
            to_write=to_write,
            resources=resources,
            environment_variables={},
            resource_io=loader_type.create_loader(toolkit_client_approval.mock_client),
            source_file=source_file,
        )
        assert not extra_files, "This tests does not support testing extra files"
        assert actual.splitlines() == expected.splitlines()
