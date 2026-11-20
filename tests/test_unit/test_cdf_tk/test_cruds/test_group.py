from collections.abc import Iterable
from copy import deepcopy
from pathlib import Path
from typing import get_args
from unittest.mock import MagicMock

import pytest
from _pytest.mark import ParameterSet
from _pytest.monkeypatch import MonkeyPatch

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.identifiers import (
    ExternalId,
    NameId,
    PostgresGatewayUserId,
    RawDatabaseId,
    RawTableId,
)
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import SpaceId
from cognite_toolkit._cdf_tk.client.resource_classes.group import (
    AllScope,
    AssetsAcl,
    GroupCapability,
    GroupRequest,
    GroupResponse,
)
from cognite_toolkit._cdf_tk.commands import DeployV2Command
from cognite_toolkit._cdf_tk.commands.auth import EnvironmentVariables
from cognite_toolkit._cdf_tk.commands.deploy_v2.command import ReadResource
from cognite_toolkit._cdf_tk.exceptions import ToolkitWrongResourceError
from cognite_toolkit._cdf_tk.resource_ios import (
    AgentIO,
    AppIO,
    AssetIO,
    DataProductIO,
    DataSetsIO,
    ExtractionPipelineIO,
    GroupAllScopedIO,
    GroupResourceScopedIO,
    IntegrationsIO,
    LocationFilterIO,
    PostgresGatewayUserIO,
    RawDatabaseIO,
    RawTableIO,
    ResourceIO,
    SAPInstanceIO,
    SecurityCategoryIO,
    SpaceIO,
    TimeSeriesIO,
)
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses
from cognite_toolkit._cdf_tk.yaml_classes import capabilities as yaml_cap
from cognite_toolkit._cdf_tk.yaml_classes.capabilities import (
    AgentExternalIdScope,
    AgentsAcl,
    AppConfigAcl,
    AppConfigScope,
    AppExternalIdScope,
    AppHostingAcl,
    AssetRootIDScope,
    Capability,
    CapabilityType,
    CurrentUserScope,
    DataDomainScope,
    DataModelInstancesAcl,
    DataModelsAcl,
    DataProductsAcl,
    DataProductScope,
    DataSetsAcl,
    DataSetScope,
    EventsAcl,
    ExperimentsAcl,
    ExperimentScope,
    ExternalIdScope,
    ExtractionConfigsAcl,
    ExtractionPipelinesAcl,
    ExtractionPipelineScope,
    ExtractionsRunAcl,
    FilesAcl,
    GroupsAcl,
    IDScope,
    IDScopeLowerCase,
    InstancesScope,
    IntegrationActionsAcl,
    IntegrationConfigsAcl,
    IntegrationsAcl,
    LabelsAcl,
    LocationFiltersAcl,
    PartitionScope,
    PostgresGatewayAcl,
    PostgresGatewayUsersScope,
    RawAcl,
    RelationshipsAcl,
    RoboticsAcl,
    SAPWritebackAcl,
    SAPWritebackRequestsAcl,
    Scope,
    SecurityCategoriesAcl,
    SeismicAcl,
    SequencesAcl,
    SignalsCurrentUserScope,
    SimulatorsAcl,
    SpaceIDScope,
    StreamRecordsAcl,
    SubscribeSignalsAcl,
    TableScope,
    TemplateGroupsAcl,
    TemplateInstancesAcl,
    ThreeDAcl,
    TimeSeriesAcl,
    TimeSeriesSubscriptionsAcl,
    TransformationsAcl,
    TransformationsExternalDataSourcesAcl,
    UnknownCapability,
    VideoStreamingAcl,
    WorkflowOrchestrationAcl,
)
from cognite_toolkit._cdf_tk.yaml_classes.groups import ExternalGroupYAML
from tests.data import LOAD_DATA
from tests.test_unit.approval_client import ApprovalToolkitClient

_DATA_SET_SCOPED_ACLS: tuple[type[Capability], ...] = (
    yaml_cap.AssetsAcl,
    EventsAcl,
    ExtractionConfigsAcl,
    ExtractionPipelinesAcl,
    ExtractionsRunAcl,
    FilesAcl,
    LabelsAcl,
    RelationshipsAcl,
    RoboticsAcl,
    SequencesAcl,
    SimulatorsAcl,
    TemplateGroupsAcl,
    TemplateInstancesAcl,
    ThreeDAcl,
    TimeSeriesAcl,
    TimeSeriesSubscriptionsAcl,
    TransformationsAcl,
    TransformationsExternalDataSourcesAcl,
    VideoStreamingAcl,
    WorkflowOrchestrationAcl,
)


def group_acl_expected_dependency() -> Iterable[ParameterSet]:
    """Test cases for all non-all scopes of all ACLs and the dependencies they are expected to give."""
    for acl_cls in _DATA_SET_SCOPED_ACLS:
        yield pytest.param(
            acl_cls.model_validate({"actions": ["READ"], "scope": DataSetScope(ids=["my_data_set"])}),
            [(DataSetsIO, ExternalId(external_id="my_data_set"))],
            id=f"{acl_cls.__name__} with DataSetScope depends on DataSetsIO",
        )
    for acl_cls in (ExtractionConfigsAcl, ExtractionsRunAcl):
        yield pytest.param(
            acl_cls(actions=["READ"], scope=ExtractionPipelineScope(ids=["my_pipeline"])),
            [(ExtractionPipelineIO, ExternalId(external_id="my_pipeline"))],
            id=f"{acl_cls.__name__} with ExtractionPipelineScope depends on ExtractionPipelineIO",
        )
    for acl_cls in (DataModelInstancesAcl, DataModelsAcl, StreamRecordsAcl):
        yield pytest.param(
            acl_cls(actions=["READ"], scope=SpaceIDScope.model_validate({"spaceIds": ["my_space"]})),
            [(SpaceIO, SpaceId(space="my_space"))],
            id=f"{acl_cls.__name__} with SpaceIDScope depends on SpaceIO",
        )
    for acl_cls in (IntegrationActionsAcl, IntegrationConfigsAcl, IntegrationsAcl):
        yield pytest.param(
            acl_cls(actions=["READ"], scope=ExternalIdScope.model_validate({"externalIds": ["my_integration"]})),
            [(IntegrationsIO, ExternalId(external_id="my_integration"))],
            id=f"{acl_cls.__name__} with ExternalIdScope depends on IntegrationsIO",
        )
        yield pytest.param(
            acl_cls(actions=["READ"], scope=DataDomainScope.model_validate({"externalIds": ["my_domain"]})),
            [],
            id=f"{acl_cls.__name__} with DataDomainScope has no dependencies",
        )
    for acl_cls in (SAPWritebackAcl, SAPWritebackRequestsAcl):
        yield pytest.param(
            acl_cls(actions=["WRITE"], scope=InstancesScope(instances=["my_sap_instance"])),
            [(SAPInstanceIO, ExternalId(external_id="my_sap_instance"))],
            id=f"{acl_cls.__name__} with InstancesScope depends on SAPInstanceIO",
        )
    yield pytest.param(
        AgentsAcl(actions=["READ"], scope=AgentExternalIdScope.model_validate({"externalIds": ["agent1"]})),
        [(AgentIO, ExternalId(external_id="agent1"))],
        id="AgentsAcl with AgentExternalIdScope depends on AgentIO",
    )
    yield pytest.param(
        AppConfigAcl(actions=["READ"], scope=AppConfigScope(apps=["SEARCH"])),
        [],
        id="AppConfigAcl with AppConfigScope has no dependencies",
    )
    yield pytest.param(
        AppHostingAcl(actions=["READ"], scope=AppExternalIdScope.model_validate({"externalIds": ["my_app"]})),
        [(AppIO, ExternalId(external_id="my_app"))],
        id="AppHostingAcl with AppExternalIdScope depends on AppIO",
    )
    yield pytest.param(
        DataProductsAcl(actions=["READ"], scope=DataProductScope.model_validate({"externalIds": ["my_data_product"]})),
        [(DataProductIO, ExternalId(external_id="my_data_product"))],
        id="DataProductsAcl with DataProductScope depends on DataProductIO",
    )
    yield pytest.param(
        DataSetsAcl(actions=["READ"], scope=IDScope(ids=["my_data_set"])),
        [(DataSetsIO, ExternalId(external_id="my_data_set"))],
        id="DataSetsAcl with IDScope depends on DataSetsIO",
    )
    yield pytest.param(
        ExtractionPipelinesAcl(actions=["READ"], scope=IDScope(ids=["my_pipeline"])),
        [(ExtractionPipelineIO, ExternalId(external_id="my_pipeline"))],
        id="ExtractionPipelinesAcl with IDScope depends on ExtractionPipelineIO",
    )
    yield pytest.param(
        LocationFiltersAcl(actions=["READ"], scope=IDScope(ids=["my_location"])),
        [(LocationFilterIO, ExternalId(external_id="my_location"))],
        id="LocationFiltersAcl with IDScope depends on LocationFilterIO",
    )
    yield pytest.param(
        SecurityCategoriesAcl(actions=["MEMBEROF"], scope=IDScopeLowerCase(ids=["my_category"])),
        [(SecurityCategoryIO, NameId(name="my_category"))],
        id="SecurityCategoriesAcl with IDScopeLowerCase depends on SecurityCategoryIO",
    )
    yield pytest.param(
        TimeSeriesAcl(actions=["READ"], scope=IDScopeLowerCase(ids=["my_timeseries"])),
        [(TimeSeriesIO, ExternalId(external_id="my_timeseries"))],
        id="TimeSeriesAcl with IDScopeLowerCase depends on TimeSeriesIO",
    )
    yield pytest.param(
        TimeSeriesAcl(actions=["READ"], scope=AssetRootIDScope.model_validate({"rootIds": ["my_root_asset"]})),
        [(AssetIO, ExternalId(external_id="my_root_asset"))],
        id="TimeSeriesAcl with AssetRootIDScope depends on AssetIO",
    )
    yield pytest.param(
        RawAcl(actions=["READ"], scope=TableScope.model_validate({"dbsToTables": {"my_db": ["my_table"]}})),
        [
            (RawDatabaseIO, RawDatabaseId(name="my_db")),
            (RawTableIO, RawTableId(db_name="my_db", name="my_table")),
        ],
        id="RawAcl with TableScope depends on RawDatabaseIO and RawTableIO",
    )
    yield pytest.param(
        GroupsAcl(actions=["READ"], scope=CurrentUserScope()),
        [],
        id="GroupsAcl with CurrentUserScope has no dependencies",
    )
    yield pytest.param(
        SubscribeSignalsAcl(actions=["READ"], scope=SignalsCurrentUserScope()),
        [],
        id="SubscribeSignalsAcl with SignalsCurrentUserScope has no dependencies",
    )
    yield pytest.param(
        SeismicAcl(actions=["READ"], scope=PartitionScope.model_validate({"partitionIds": [1]})),
        [],
        id="SeismicAcl with PartitionScope has no dependencies",
    )
    yield pytest.param(
        ExperimentsAcl(actions=["USE"], scope=ExperimentScope(experiments=["my_experiment"])),
        [],
        id="ExperimentsAcl with ExperimentScope has no dependencies",
    )
    yield pytest.param(
        PostgresGatewayAcl(actions=["READ"], scope=PostgresGatewayUsersScope(usernames=["my_user"])),
        [(PostgresGatewayUserIO, PostgresGatewayUserId(username="my_user"))],
        id="PostgresGatewayAcl with PostgresGatewayUsersScope",
    )


def _possible_scopes(capability_cls: type[Capability]) -> set[type[Scope]]:
    annotation = capability_cls.model_fields["scope"].annotation
    return set(get_args(annotation) or (annotation,))


class TestGroupIO:
    def test_load_all_scoped_only(self, env_vars_with_client: EnvironmentVariables, monkeypatch: MonkeyPatch):
        loader = GroupAllScopedIO(env_vars_with_client.get_client())
        raw_list = loader.load_resource_file(
            LOAD_DATA / "auth" / "1.my_group_unscoped.yaml", env_vars_with_client.dump()
        )
        loaded = loader.load_resource(raw_list[0], is_dry_run=False)
        assert loaded.name == "unscoped_group_name"

        raw_list = loader.load_resource_file(LOAD_DATA / "auth" / "1.my_group_scoped.yaml", env_vars_with_client.dump())
        with pytest.raises(ToolkitWrongResourceError):
            loader.load_resource(raw_list[0], is_dry_run=False)

    def test_load_resource_scoped_only(self, env_vars_with_client: EnvironmentVariables, monkeypatch: MonkeyPatch):
        loader = GroupResourceScopedIO(env_vars_with_client.get_client())
        with pytest.raises(ToolkitWrongResourceError):
            raw_list = loader.load_resource_file(
                LOAD_DATA / "auth" / "1.my_group_unscoped.yaml", env_vars_with_client.dump()
            )
            loader.load_resource(raw_list[0], is_dry_run=False)

        raw_list = loader.load_resource_file(LOAD_DATA / "auth" / "1.my_group_scoped.yaml", env_vars_with_client.dump())
        loaded = loader.load_resource(raw_list[0], is_dry_run=False)
        assert loaded.name == "scoped_group_name"
        assert loaded.capabilities is not None
        assert len(loaded.capabilities) == 4

        caps = {type(cap.acl).__name__: cap.acl for cap in loaded.capabilities}

        assert all(isinstance(item, int) for item in caps["DataSetsAcl"].scope.model_dump()["ids"])
        assert all(isinstance(item, int) for item in caps["AssetsAcl"].scope.model_dump()["ids"])
        assert all(isinstance(item, int) for item in caps["ExtractionConfigsAcl"].scope.model_dump()["ids"])
        assert caps["SessionsAcl"].scope.scope_name == "all"

    def test_load_group_list_resource_scoped_only(
        self, env_vars_with_client: EnvironmentVariables, monkeypatch: MonkeyPatch
    ):
        loader = GroupResourceScopedIO(env_vars_with_client.get_client())
        raw_list = loader.load_resource_file(
            LOAD_DATA / "auth" / "1.my_group_list_combined.yaml", env_vars_with_client.dump()
        )
        loaded = loader.load_resource(raw_list[0], is_dry_run=False)

        assert isinstance(loaded, GroupRequest)
        assert loaded.name == "scoped_group_name"

    def test_load_group_list_all_scoped_only(
        self, env_vars_with_client: EnvironmentVariables, monkeypatch: MonkeyPatch
    ):
        loader = GroupAllScopedIO(env_vars_with_client.get_client())
        raw_list = loader.load_resource_file(
            LOAD_DATA / "auth" / "1.my_group_list_combined.yaml", env_vars_with_client.dump()
        )
        loaded = loader.load_resource(raw_list[1], is_dry_run=False)

        assert isinstance(loaded, GroupRequest)
        assert loaded.name == "unscoped_group_name"

    def test_unchanged_new_group(
        self,
        env_vars_with_client: EnvironmentVariables,
        toolkit_client_approval: ApprovalToolkitClient,
        monkeypatch: MonkeyPatch,
    ) -> None:
        loader = GroupResourceScopedIO(env_vars_with_client.get_client())
        filepath = LOAD_DATA / "auth" / "1.my_group_scoped.yaml"
        raw_list = loader.load_resource_file(filepath, env_vars_with_client.dump())
        loaded = loader.load_resource(deepcopy(raw_list[0]), is_dry_run=False)
        assert loaded.capabilities is not None

        # Simulate that one group is already in CDF
        cdf_group = GroupResponse(
            id=123,
            name=loaded.name,
            source_id=loaded.source_id,
            capabilities=loaded.capabilities,
            metadata=loaded.metadata,
            is_deleted=False,
        )
        toolkit_client_approval.append(GroupResponse, [cdf_group])

        new_group = GroupRequest(name="new_group", source_id="123", capabilities=loaded.capabilities)
        new_file = MagicMock(spec=Path)
        new_file.read_text.return_value = new_group.dump_yaml()
        loaded_id = loader.get_id(loaded)
        new_raw = loader.load_resource_file(new_file, env_vars_with_client.dump())
        new_loaded = loader.load_resource(deepcopy(new_raw[0]), is_dry_run=False)
        new_id = loader.get_id(new_loaded)
        existing_list = loader.retrieve([loaded_id, new_id])
        result = DeployV2Command.categorize_resources(
            loader,
            resource_by_id={
                loaded_id: ReadResource(loaded, raw_list[0], [filepath]),
                new_id: ReadResource(new_loaded, new_raw[0], [new_file]),
            },
            cdf_by_id={loader.get_id(item): item for item in existing_list},
        )
        assert {
            "create": len(result.to_create),
            "change": len(result.to_update),
            "delete": len(result.to_delete),
            "unchanged": len(result.unchanged),
        } == {"create": 1, "change": 0, "delete": 0, "unchanged": 1}

    def test_upsert_group(
        self,
        env_vars_with_client: EnvironmentVariables,
        toolkit_client_approval: ApprovalToolkitClient,
        monkeypatch: MonkeyPatch,
    ):
        loader = GroupResourceScopedIO(env_vars_with_client.get_client())
        filepath = LOAD_DATA / "auth" / "1.my_group_scoped.yaml"
        raw_list = loader.load_resource_file(filepath, env_vars_with_client.dump())
        loaded = loader.load_resource(deepcopy(raw_list[0]), is_dry_run=False)
        assert loaded.capabilities is not None

        # Simulate that the group is already in CDF, but with fewer capabilities
        cdf_group = GroupResponse(
            id=123,
            name=loaded.name,
            source_id=loaded.source_id,
            capabilities=loaded.capabilities[0:1],
            metadata=loaded.metadata,
            is_deleted=False,
        )
        toolkit_client_approval.append(GroupResponse, [cdf_group])

        resource_id = loader.get_id(loaded)
        existing_list = loader.retrieve([resource_id])
        result = DeployV2Command.categorize_resources(
            loader,
            resource_by_id={resource_id: ReadResource(loaded, raw_list[0], [filepath])},
            cdf_by_id={resource_id: existing_list[0]},
        )

        assert {
            "create": len(result.to_create),
            "change": len(result.to_update),
            "delete": len(result.to_delete),
            "unchanged": len(result.unchanged),
        } == {"create": 0, "change": 1, "delete": 0, "unchanged": 0}

    def test_unchanged_new_group_without_metadata(
        self,
        env_vars_with_client: EnvironmentVariables,
        toolkit_client_approval: ApprovalToolkitClient,
        monkeypatch: MonkeyPatch,
    ) -> None:
        loader = GroupAllScopedIO(env_vars_with_client.get_client())
        local_group = """name: gp_no_metadata
sourceId: '123'
capabilities:
- assetsAcl:
    actions:
    - READ
    scope:
      all: {}
"""
        cdf_group = GroupResponse(
            name="gp_no_metadata",
            source_id="123",
            capabilities=[GroupCapability(acl=AssetsAcl(actions=["READ"], scope=AllScope()))],
            metadata={},
            id=3760258445038144,
            is_deleted=False,
        )

        # Simulate that one group is already in CDF
        toolkit_client_approval.append(GroupResponse, [cdf_group])
        filepath = MagicMock(spec=Path)
        filepath.read_text.return_value = local_group

        resource_dict = loader.load_resource_file(filepath, {})
        assert len(resource_dict) == 1
        resource = loader.load_resource(deepcopy(resource_dict[0]), is_dry_run=False)
        resource_id = loader.get_id(resource)
        existing_list = loader.retrieve([resource_id])
        result = DeployV2Command.categorize_resources(
            loader,
            resource_by_id={resource_id: ReadResource(resource, resource_dict[0], [filepath])},
            cdf_by_id={resource_id: existing_list[0]},
        )
        assert {
            "create": len(result.to_create),
            "change": len(result.to_update),
            "delete": len(result.to_delete),
            "unchanged": len(result.unchanged),
        } == {"create": 0, "change": 0, "delete": 0, "unchanged": 1}

    def test_unchanged_group_raw_acl_table_scoped(
        self,
        env_vars_with_client: EnvironmentVariables,
        toolkit_client_approval: ApprovalToolkitClient,
        monkeypatch: MonkeyPatch,
    ) -> None:
        loader = GroupResourceScopedIO(env_vars_with_client.get_client())
        local_group = """name: gp_raw_acl_table_scoped
sourceId: '123'
capabilities:
- rawAcl:
   actions:
   - READ
   scope:
     tableScope:
       dbsToTables:
         'db_name':
           - labels
        """
        cdf_group = GroupResponse(
            name="gp_raw_acl_table_scoped",
            source_id="123",
            capabilities=[
                {
                    "rawAcl": {
                        "actions": ["READ"],
                        "scope": {
                            "tableScope": {
                                "dbsToTables": {
                                    "db_name": ["labels"],
                                }
                            }
                        },
                    }
                }
            ],
            metadata={},
            id=3760258445038144,
            is_deleted=False,
        )

        # Simulate that one group is already in CDF
        toolkit_client_approval.append(GroupResponse, [cdf_group])
        filepath = MagicMock(spec=Path)
        filepath.read_text.return_value = local_group

        resource_dict = loader.load_resource_file(filepath, {})
        assert len(resource_dict) == 1
        resource = loader.load_resource(deepcopy(resource_dict[0]), is_dry_run=False)
        resource_id = loader.get_id(resource)
        existing_list = loader.retrieve([resource_id])
        result = DeployV2Command.categorize_resources(
            loader,
            resource_by_id={resource_id: ReadResource(resource, resource_dict[0], [filepath])},
            cdf_by_id={resource_id: existing_list[0]},
        )
        assert {
            "create": len(result.to_create),
            "change": len(result.to_update),
            "delete": len(result.to_delete),
            "unchanged": len(result.unchanged),
        } == {"create": 0, "change": 0, "delete": 0, "unchanged": 1}

    def test_dependencies_test_cases_is_complete(self) -> None:
        """Test that the test cases cover every non-all scope of every ACL."""
        acls = [case.values[0] for case in group_acl_expected_dependency()]
        covered = {(type(acl), type(acl.scope)) for acl in acls if isinstance(acl, Capability)}
        expected = {
            (capability_cls, scope_cls)
            for capability_cls in get_concrete_subclasses(Capability)
            if capability_cls is not UnknownCapability
            for scope_cls in _possible_scopes(capability_cls)
            if scope_cls is not yaml_cap.AllScope
        }

        missing = sorted(f"{acl.__name__} with {scope.__name__}" for acl, scope in expected - covered)
        assert not missing, f"Missing test cases in group_acl_expected_dependency: {missing}"

    @pytest.mark.parametrize("acl, expected", list(group_acl_expected_dependency()))
    def test_get_dependencies(self, acl: CapabilityType, expected: list[tuple[type[ResourceIO], Identifier]]):
        group = ExternalGroupYAML.model_validate(
            {"name": "external_group", "sourceId": "external_source_id", "capabilities": [acl]},
        )

        actual = list(GroupResourceScopedIO.get_dependencies(group))
        assert actual == expected
