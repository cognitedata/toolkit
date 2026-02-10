from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import ViewReference
from cognite_toolkit._cdf_tk.commands.pull import PullCommand, ResourceYAMLDifference, TextFileDifference
from cognite_toolkit._cdf_tk.cruds import DataSetsCRUD, ViewCRUD
from cognite_toolkit._cdf_tk.data_classes import (
    BuildVariable,
    BuildVariables,
    BuiltFullResourceList,
    BuiltResourceFull,
)
from tests.test_unit.approval_client import ApprovalToolkitClient


@dataclass
class ExpectedDiff:
    """Expected differences from comparing local and CDF resources."""

    added: dict[str, Any] = field(default_factory=dict)
    changed: dict[str, Any] = field(default_factory=dict)
    cannot_change: dict[str, Any] = field(default_factory=dict)


def load_update_diffs_use_cases():
    build_file = """externalId: tr_pump_asset_hierarchy-load-collections_pump
name: pump_asset_hierarchy-load-collections_pump
destination:
  type: asset_hierarchy
dataSetExternalId: src:lift_pump_stations
ignoreNullFields: false
# Specify credentials separately like this:
# You can also use different credentials for the running transformations than the ones you use to deploy
authentication:
  clientId: ${IDP_CLIENT_ID}
  clientSecret: ${IDP_CLIENT_SECRET}
  tokenUri: ${IDP_TOKEN_URL}
  # Optional: If idP requires providing the scopes
  cdfProjectName: ${CDF_PROJECT}
  scopes:
  - ${IDP_SCOPES}
  # Optional: If idP requires providing the audience
  audience: ${IDP_AUDIENCE}
"""
    source_file = """externalId: tr_pump_asset_hierarchy-load-collections_pump
name: pump_asset_hierarchy-load-collections_pump
destination:
  type: asset_hierarchy
dataSetExternalId: {{data_set}}
ignoreNullFields: false
# Specify credentials separately like this:
# You can also use different credentials for the running transformations than the ones you use to deploy
authentication:
  clientId: {{cicd_clientId}}
  clientSecret: {{cicd_clientSecret}}
  tokenUri: {{cicd_tokenUri}}
  # Optional: If idP requires providing the scopes
  cdfProjectName: {{cdfProjectName}}
  scopes: {{cicd_scopes}}
  # Optional: If idP requires providing the audience
  audience: {{cicd_audience}}
"""
    cdf_resource = {
        "conflictMode": "upsert",
        "destination": {"type": "asset_hierarchy"},
        "externalId": "tr_pump_asset_hierarchy-load-collections_pump",
        "ignoreNullFields": False,
        "isPublic": True,
        "name": "pump_asset_hierarchy-load-collections_pump",
    }
    expected = ExpectedDiff(
        added={"isPublic": True, "conflictMode": "upsert"},
    )

    dumped = """externalId: tr_pump_asset_hierarchy-load-collections_pump
name: pump_asset_hierarchy-load-collections_pump
destination:
  type: asset_hierarchy
dataSetExternalId: {{data_set}}
ignoreNullFields: false
# Specify credentials separately like this:
# You can also use different credentials for the running transformations than the ones you use to deploy
authentication:
  clientId: {{cicd_clientId}}
  clientSecret: {{cicd_clientSecret}}
  tokenUri: {{cicd_tokenUri}}
  # Optional: If idP requires providing the scopes
  cdfProjectName: {{cdfProjectName}}
  scopes: {{cicd_scopes}}
  # Optional: If idP requires providing the audience
  audience: {{cicd_audience}}
conflictMode: upsert
isPublic: true
"""

    yield pytest.param(build_file, source_file, cdf_resource, expected, dumped, id="Transformation with no differences")

    build_file = """autoCreateDirectRelations: true
skipOnVersionConflict: false
replace: true
nodes:
- space: APM_Config
  externalId: default_infield_config_minimal
  sources:
  - source:
      space: APM_Config
      externalId: APM_Config
      version: '1'
      type: view
    properties:
      featureConfiguration:
        rootLocationConfigurations:
        - assetExternalId: WMT:VAL
          appDataInstanceSpace: sp_infield_oid_app_data
          sourceDataInstanceSpace: sp_asset_oid_source
          templateAdmins:
          - gp_infield_oid_template_admins
          checklistAdmins:
          - gp_infield_oid_checklist_admins
      customerDataSpaceId: APM_SourceData
      customerDataSpaceVersion: '1'
      name: Default location
"""
    source_file = """autoCreateDirectRelations: true
skipOnVersionConflict: false
replace: true
nodes:
- space: {{apm_config_instance_space}}
  externalId: default_infield_config_minimal
  sources:
  - source:
      space: APM_Config
      externalId: APM_Config
      version: '1'
      type: view
    properties:
      featureConfiguration:
        rootLocationConfigurations:
        - assetExternalId: {{root_asset_external_id}}
          appDataInstanceSpace: sp_infield_{{default_location}}_app_data
          sourceDataInstanceSpace: sp_asset_{{default_location}}_source
          templateAdmins:
          - gp_infield_{{default_location}}_template_admins
          checklistAdmins:
          - gp_infield_{{default_location}}_checklist_admins
      customerDataSpaceId: APM_SourceData
      customerDataSpaceVersion: '1'
      name: Default location
"""

    cdf_resource = {
        "autoCreateDirectRelations": True,
        "skipOnVersionConflict": False,
        "replace": True,
        "nodes": [
            {
                "space": "APM_Config",
                "instanceType": "node",
                "externalId": "default_infield_config_minimal",
                "sources": [
                    {
                        "properties": {
                            "name": "Default location",
                            "appDataSpaceVersion": "1",
                            "customerDataSpaceId": "APM_SourceData",
                            "featureConfiguration": {
                                "rootLocationConfigurations": [
                                    {
                                        "templateAdmins": ["gp_infield_oid_template_admins"],
                                        "assetExternalId": "WMT:VAL",
                                        "checklistAdmins": ["gp_infield_oid_checklist_admins"],
                                        "appDataInstanceSpace": "sp_infield_oid_app_data",
                                        "sourceDataInstanceSpace": "sp_asset_oid_source",
                                    }
                                ]
                            },
                            "customerDataSpaceVersion": "1",
                        },
                        "source": {"space": "APM_Config", "externalId": "APM_Config", "version": "1", "type": "view"},
                    }
                ],
            }
        ],
    }

    expected = ExpectedDiff(
        added={
            "nodes.0.instanceType": "node",
            "nodes.0.sources.0.properties.appDataSpaceVersion": "1",
        },
    )

    dumped = """autoCreateDirectRelations: true
skipOnVersionConflict: false
replace: true
nodes:
- space: {{apm_config_instance_space}}
  externalId: default_infield_config_minimal
  sources:
  - source:
      space: APM_Config
      externalId: APM_Config
      version: '1'
      type: view
    properties:
      featureConfiguration:
        rootLocationConfigurations:
        - assetExternalId: {{root_asset_external_id}}
          appDataInstanceSpace: sp_infield_{{default_location}}_app_data
          sourceDataInstanceSpace: sp_asset_{{default_location}}_source
          templateAdmins:
          - gp_infield_{{default_location}}_template_admins
          checklistAdmins:
          - gp_infield_{{default_location}}_checklist_admins
      customerDataSpaceId: APM_SourceData
      customerDataSpaceVersion: '1'
      name: Default location
      appDataSpaceVersion: '1'
  instanceType: node
"""

    yield pytest.param(build_file, source_file, cdf_resource, expected, dumped, id="Node with a changed field")

    build_file = """externalId: tr_timeseries_oid_pi_apm_simple_load_timeseries2assets
name: timeseries:oid:pi:apm_simple:load_timeseries2assets
destination:
  dataModel:
    space: sp_apm_simple
    externalId: apm_simple
    version: '1'
    destinationType: Asset
  instanceSpace: sp_apm_simple
  type: instances
ignoreNullFields: true
shared: true
action: upsert
dataSetExternalId: ds_transformations_oid
# Specify credentials separately like this:
# You can also use different credentials for the running transformations than the ones you use to deploy
authentication:
  clientId: ${IDP_CLIENT_ID}
  clientSecret: ${IDP_CLIENT_SECRET}
  tokenUri: ${IDP_TOKEN_URL}
  # Optional: If idP requires providing the cicd_scopes
  cdfProjectName: ${CDF_PROJECT}
  scopes: ['${IDP_SCOPES}']
  # Optional: If idP requires providing the cicd_audience
  audience: ${IDP_AUDIENCE}"""

    source_file = """externalId: tr_timeseries_{{default_location}}_{{source_timeseries}}_apm_simple_load_timeseries2assets
name: timeseries:{{default_location}}:{{source_timeseries}}:apm_simple:load_timeseries2assets
destination:
  dataModel:
    space: {{space}}
    externalId: {{datamodel}}
    version: '1'
    destinationType: Asset
  instanceSpace: {{space}}
  type: instances
ignoreNullFields: true
shared: true
action: upsert
dataSetExternalId: ds_transformations_{{default_location}}
# Specify credentials separately like this:
# You can also use different credentials for the running transformations than the ones you use to deploy
authentication:
  clientId: {{cicd_clientId}}
  clientSecret: {{cicd_clientSecret}}
  tokenUri: {{cicd_tokenUri}}
  # Optional: If idP requires providing the cicd_scopes
  cdfProjectName: {{cdfProjectName}}
  scopes: {{cicd_scopes}}
  # Optional: If idP requires providing the cicd_audience
  audience: {{cicd_audience}}"""

    cdf_resource = {
        "externalId": "tr_timeseries_oid_pi_apm_simple_load_timeseries2assets",
        "name": "timeseries:oid:pi:apm_simple:load_timeseries2assets",
        "destination": {
            "type": "instances",
            "dataModel": {
                "space": "sp_apm_simple",
                "externalId": "apm_simple",
                "version": "1",
                "destinationType": "Asset",
                "destinationRelationshipFromType": None,
            },
            "instanceSpace": "sp_apm_simple",
        },
        "conflictMode": "upsert",
        "isPublic": True,
        "ignoreNullFields": True,
    }

    expected = ExpectedDiff(
        added={"conflictMode": "upsert", "isPublic": True},
    )

    dumped = """externalId: tr_timeseries_{{default_location}}_{{source_timeseries}}_apm_simple_load_timeseries2assets
name: timeseries:{{default_location}}:{{source_timeseries}}:apm_simple:load_timeseries2assets
destination:
  dataModel:
    space: {{space}}
    externalId: {{datamodel}}
    version: '1'
    destinationType: Asset
    destinationRelationshipFromType: null
  instanceSpace: {{space}}
  type: instances
ignoreNullFields: true
shared: true
action: upsert
dataSetExternalId: ds_transformations_{{default_location}}
# Specify credentials separately like this:
# You can also use different credentials for the running transformations than the ones you use to deploy
authentication:
  clientId: {{cicd_clientId}}
  clientSecret: {{cicd_clientSecret}}
  tokenUri: {{cicd_tokenUri}}
  # Optional: If idP requires providing the cicd_scopes
  cdfProjectName: {{cdfProjectName}}
  scopes: {{cicd_scopes}}
  # Optional: If idP requires providing the cicd_audience
  audience: {{cicd_audience}}
conflictMode: upsert
isPublic: true
"""

    yield pytest.param(build_file, source_file, cdf_resource, expected, dumped, id="Transformation double variable.")


class TestResourceYAML:
    @pytest.mark.parametrize(
        "build_file, source_file, cdf_resource, expected, expected_dumped",
        list(load_update_diffs_use_cases()),
    )
    def test_load_update_changes_dump(
        self,
        build_file: str,
        source_file: str,
        cdf_resource: dict[str, Any],
        expected: ExpectedDiff,
        expected_dumped: str,
    ) -> None:
        resource_yaml = ResourceYAMLDifference.load(build_file, source_file)
        resource_yaml.update_cdf_resource(cdf_resource)

        added = {".".join(map(str, key)): value.cdf_value for key, value in resource_yaml.items() if value.is_added}
        changed = {".".join(map(str, key)): value.cdf_value for key, value in resource_yaml.items() if value.is_changed}
        cannot_change = {
            ".".join(map(str, key)): value.cdf_value for key, value in resource_yaml.items() if value.is_cannot_change
        }

        assert added == expected.added
        assert changed == expected.changed
        assert cannot_change == expected.cannot_change

        assert resource_yaml.dump_yaml_with_comments() == expected_dumped


def load_update_changed_dump_test_cases():
    build_file = """--- 1. asset root (defining all columns)
SELECT
    "Lift Pump Stations" AS name,
    dataset_id("src:lift_pump_stations") AS dataSetId,
    "lift_pump_stations:root" AS externalId,
    '' as parentExternalId,
    "An example pump dataset" as description,
    null as metadata
"""
    source_file = """--- 1. asset root (defining all columns)
SELECT
    "Lift Pump Stations" AS name,
    dataset_id("{{data_set}}") AS dataSetId,
    "lift_pump_stations:root" AS externalId,
    '' as parentExternalId,
    "An example pump dataset" as description,
    null as metadata
"""
    cdf_content = """--- 1. asset root (defining all columns) And Extra comment in the SQL
SELECT
    "Lift Pump Stations" AS name,
    dataset_id("src:new_data_set") AS dataSetId,
    "lift_pump_stations:root" AS externalId,
    '' as parentExternalId,
    "An example pump dataset" as description,
    null as metadata
"""
    expected = {
        "added": [],
        "changed": ["--- 1. asset root (defining all columns) And Extra comment in the SQL"],
        "cannot_change": [('    dataset_id("src:new_data_set") AS dataSetId,', ["data_set"])],
    }
    dumped = """--- 1. asset root (defining all columns) And Extra comment in the SQL
SELECT
    "Lift Pump Stations" AS name,
    dataset_id("{{data_set}}") AS dataSetId,
    "lift_pump_stations:root" AS externalId,
    '' as parentExternalId,
    "An example pump dataset" as description,
    null as metadata
"""
    yield pytest.param(build_file, source_file, cdf_content, expected, dumped, id="SQL with one line differences")


class TestTextFileDifference:
    @pytest.mark.parametrize(
        "build_file, source_file, cdf_content, expected, dumped",
        list(load_update_changed_dump_test_cases()),
    )
    def test_load_update_changes_dump(
        self, build_file: str, source_file: str, cdf_content: str, expected: dict[str, list], dumped: str
    ) -> None:
        text_file = TextFileDifference.load(build_file, source_file)
        text_file.update_cdf_content(cdf_content)

        added = [line.cdf_value for line in text_file if line.is_added]
        changed = [line.cdf_value for line in text_file if line.is_changed]
        cannot_change = [(line.cdf_value, line.variables) for line in text_file if line.is_cannot_change]

        assert added == expected["added"]
        assert changed == expected["changed"]
        assert cannot_change == expected["cannot_change"]

        assert text_file.dump() == dumped


def to_write_content_use_cases() -> Iterable:
    source = """name: Ingestion
externalId: {{ dataset }}
description: This dataset contains Transformations, Functions, and Workflows for ingesting data into Cognite Data Fusion.
"""
    to_write = {"ingestion": {"name": "Ingestion", "externalId": "ingestion", "description": "New description"}}
    variable = BuildVariable(
        key="dataset",
        value="ingestion",
        is_selected=True,
        location=Path("whatever"),
    )
    ingestion = MagicMock(spec=BuiltResourceFull)
    ingestion.build_variables = BuildVariables([variable])
    ingestion.identifier = "ingestion"
    ingestion.extra_sources = []

    resources = BuiltFullResourceList([ingestion])

    expected = """name: Ingestion
externalId: {{ dataset }}
description: New description
"""

    yield pytest.param(
        source, to_write, resources, expected, DataSetsCRUD, Path("my.DataSet.yaml"), id="One resource changed"
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
        DataSetsCRUD,
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
        "unique_dataset": {"name": "Another", "externalId": "unique_dataset", "description": "also new description"},
    }
    unique_dataset = MagicMock(spec=BuiltResourceFull)
    unique_dataset.build_variables = BuildVariables([])
    unique_dataset.identifier = "unique_dataset"
    unique_dataset.extra_sources = []
    resources = BuiltFullResourceList([ingestion, unique_dataset])

    yield pytest.param(
        source,
        to_write_multi,
        resources,
        expected,
        DataSetsCRUD,
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
        ViewReference(space="my_space", external_id="my_external_id", version="v1"): {
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
        key="instance_space",
        value="my_space",
        is_selected=True,
        location=Path("whatever"),
    )
    view_resource = MagicMock(spec=BuiltResourceFull)
    view_resource.build_variables = BuildVariables([variable_view])
    view_resource.identifier = ViewReference(space="my_space", external_id="my_external_id", version="v1")
    view_resource.extra_sources = []

    resources_view = BuiltFullResourceList([view_resource])

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
        ViewCRUD,
        Path("my.View.yaml"),
        id="View with filter and no differences",
    )


class TestPullCommand:
    @pytest.mark.parametrize(
        "source, to_write, resources, expected, loader_type, source_file",
        list(to_write_content_use_cases()),
    )
    def test_to_write_content(
        self,
        source: str,
        to_write: dict[str, [dict[str, Any]]],
        resources: BuiltFullResourceList,
        expected: str,
        loader_type: type,
        source_file: Path,
        toolkit_client_approval: ApprovalToolkitClient,
    ) -> None:
        cmd = PullCommand(silent=True, skip_tracking=True)

        actual, extra_files = cmd._to_write_content(
            source=source,
            to_write=to_write,
            resources=resources,
            environment_variables={},
            loader=loader_type.create_loader(toolkit_client_approval.mock_client),
            source_file=source_file,
        )
        assert not extra_files, "This tests does not support testing extra files"
        assert actual.splitlines() == expected.splitlines()
