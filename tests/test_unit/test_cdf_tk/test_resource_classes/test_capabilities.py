from collections.abc import Iterable
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes.capabilities import Capability
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic


def all_acls() -> Iterable:
    acl_list = [
        {"agentsAcl": {"actions": ["READ", "WRITE", "RUN"], "scope": {"all": {}}}},
        {"annotationsAcl": {"actions": ["WRITE", "READ", "SUGGEST", "REVIEW"], "scope": {"all": {}}}},
        {"appConfigAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"appConfigAcl": {"actions": ["READ", "WRITE"], "scope": {"appScope": {"apps": ["SEARCH"]}}}},
        {"assetsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"assetsAcl": {"actions": ["READ", "WRITE"], "scope": {"datasetScope": {"ids": ["myDataSet"]}}}},
        {"auditlogAcl": {"actions": ["READ"], "scope": {"all": {}}}},
        {"dataModelInstancesAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"dataModelInstancesAcl": {"actions": ["READ"], "scope": {"spaceIdScope": {"spaceIds": ["maintain"]}}}},
        {
            "dataModelInstancesAcl": {
                "actions": ["WRITE_PROPERTIES"],
                "scope": {"spaceIdScope": {"spaceIds": ["tech-space"]}},
            }
        },
        {"dataModelsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {
            "dataModelsAcl": {
                "actions": ["READ"],
                "scope": {"spaceIdScope": {"spaceIds": ["maintain", "main-data"]}},
            }
        },
        {"datasetsAcl": {"actions": ["READ", "WRITE", "OWNER"], "scope": {"all": {}}}},
        {"datasetsAcl": {"actions": ["READ", "WRITE", "OWNER"], "scope": {"idScope": {"ids": ["my_dataset"]}}}},
        {"diagramParsingAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"digitalTwinAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"documentFeedbackAcl": {"actions": ["CREATE", "READ", "DELETE"], "scope": {"all": {}}}},
        {"documentPipelinesAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"entitymatchingAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"eventsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"eventsAcl": {"actions": ["READ", "WRITE"], "scope": {"datasetScope": {"ids": ["myDataSet"]}}}},
        {"eventsAcl": {"actions": ["READ"], "scope": {"datasetScope": {"ids": ["myDataSet", "myDataSet2"]}}}},
        {"extractionConfigsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"extractionPipelinesAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {
            "extractionPipelinesAcl": {
                "actions": ["READ", "WRITE"],
                "scope": {"idScope": {"ids": ["myPipeline", "myPipeline"]}},
            }
        },
        {
            "extractionPipelinesAcl": {
                "actions": ["READ", "WRITE"],
                "scope": {"datasetScope": {"ids": ["myDataSet", "myDataSet2"]}},
            }
        },
        {"extractionRunsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"filePipelinesAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"filesAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"filesAcl": {"actions": ["READ", "WRITE"], "scope": {"datasetScope": {"ids": ["myDataSet", "myDataSet2"]}}}},
        {"functionsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"genericsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"groupsAcl": {"actions": ["LIST", "READ", "DELETE", "UPDATE", "CREATE"], "scope": {"all": {}}}},
        {"groupsAcl": {"actions": ["READ", "CREATE", "UPDATE", "DELETE"], "scope": {"currentuserscope": {}}}},
        {"hostedExtractorsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"labelsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"locationFiltersAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {
            "locationFiltersAcl": {
                "actions": ["READ", "WRITE"],
                "scope": {"idScope": {"ids": ["dataset", "otherDataset"]}},
            }
        },
        {"modelHostingAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"monitoringTasksAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"notificationsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"pipelinesAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {
            "postgresGatewayAcl": {
                "actions": ["READ", "WRITE"],
                "scope": {"usersScope": {"usernames": ["test_1", "test_2"]}},
            }
        },
        {"projectsAcl": {"actions": ["UPDATE", "LIST", "READ", "CREATE", "DELETE"], "scope": {"all": {}}}},
        {"rawAcl": {"actions": ["READ", "WRITE", "LIST"], "scope": {"all": {}}}},
        {
            "rawAcl": {
                "actions": ["READ", "WRITE", "LIST"],
                "scope": {"tableScope": {"dbsToTables": {"no table in this": []}}},
            }
        },
        {
            "rawAcl": {
                "actions": ["READ", "WRITE", "LIST"],
                "scope": {"tableScope": {"dbsToTables": {"test db 1": ["empty tbl", "test tbl 1"]}}},
            }
        },
        {"relationshipsAcl": {"actions": ["READ"], "scope": {"all": {}}}},
        {"relationshipsAcl": {"actions": ["READ"], "scope": {"datasetScope": {"ids": ["myDataSet", "myDataSet2"]}}}},
        {"roboticsAcl": {"actions": ["READ", "CREATE", "UPDATE", "DELETE"], "scope": {"all": {}}}},
        {"roboticsAcl": {"actions": ["READ"], "scope": {"datasetScope": {"ids": ["myDataSet"]}}}},
        {"sapWritebackAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"sapWritebackAcl": {"actions": ["READ", "WRITE"], "scope": {"instancesScope": {"instances": ["123", "456"]}}}},
        {"sapWritebackRequestsAcl": {"actions": ["WRITE", "LIST"], "scope": {"all": {}}}},
        {
            "sapWritebackRequestsAcl": {
                "actions": ["WRITE", "LIST"],
                "scope": {"instancesScope": {"instances": ["123", "456"]}},
            }
        },
        {"scheduledCalculationsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {
            "securityCategoriesAcl": {
                "actions": ["DELETE", "MEMBEROF", "LIST", "CREATE", "UPDATE"],
                "scope": {"all": {}},
            }
        },
        {
            "securityCategoriesAcl": {
                "actions": ["MEMBEROF", "LIST", "CREATE", "UPDATE", "DELETE"],
                "scope": {"idscope": {"ids": ["myCategory", "myOtherCategory"]}},
            }
        },
        {"seismicAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"seismicAcl": {"actions": ["WRITE"], "scope": {"partition": {"partitionIds": [123, 456]}}}},
        {"sequencesAcl": {"actions": ["READ"], "scope": {"all": {}}}},
        {"sequencesAcl": {"actions": ["WRITE"], "scope": {"datasetScope": {"ids": ["myDataSet", "myDataSet2"]}}}},
        {"sessionsAcl": {"actions": ["LIST", "CREATE", "DELETE"], "scope": {"all": {}}}},
        {"templateGroupsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {
            "templateGroupsAcl": {
                "actions": ["READ", "WRITE"],
                "scope": {"datasetScope": {"ids": ["myDataSet", "myDataSet2"]}},
            }
        },
        {
            "templateInstancesAcl": {
                "actions": ["READ", "WRITE"],
                "scope": {"datasetScope": {"ids": ["myDataSet", "myDataSet2"]}},
            }
        },
        {"templateInstancesAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"threedAcl": {"actions": ["READ", "CREATE", "UPDATE", "DELETE"], "scope": {"all": {}}}},
        {"timeSeriesAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {
            "timeSeriesAcl": {
                "actions": ["READ", "WRITE"],
                "scope": {"datasetScope": {"ids": ["myDataSet", "myDataSet2"]}},
            }
        },
        {"timeSeriesAcl": {"actions": ["READ"], "scope": {"idscope": {"ids": ["myTimeseries", "myOtherTimeseries"]}}}},
        {"timeSeriesAcl": {"actions": ["WRITE", "READ"], "scope": {"assetRootIdScope": {"rootIds": ["myAsset"]}}}},
        {"transformationsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"transformationsAcl": {"actions": ["READ", "WRITE"], "scope": {"datasetScope": {"ids": ["myDataSet"]}}}},
        {"visionModelAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"wellsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"workflowOrchestrationAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {
            "workflowOrchestrationAcl": {
                "actions": ["READ", "WRITE"],
                "scope": {"datasetScope": {"ids": ["myDataSet", "myDataSet2"]}},
            }
        },
    ]

    yield from (pytest.param(acl, id=next(iter(acl.keys()))) for acl in acl_list)


class TestCapabilities:
    @pytest.mark.parametrize("acl", all_acls())
    def test_load_dump_capability(self, acl: dict[str, object]) -> None:
        capability = Capability.model_validate(acl)
        assert capability.model_dump(by_alias=True, exclude_unset=True) == acl

    @pytest.mark.parametrize(
        "data, expected_errors",
        [
            pytest.param(
                {"datasetsAcl": {"actions": ["READ", "WRITE", "OWNER"], "scope": {"idscope": {"ids": ["my_dataset"]}}}},
                ["In field scope invalid scope name 'idscope'. Expected all or idScope"],
                id="Wrong case for datasetsAcl idScope",
            ),
            pytest.param(
                {
                    "extractionPipelinesAcl": {
                        "actions": ["OWNER", "READ"],
                        "scope": {"idscope": {"ids": ["my_pipeline"]}},
                    }
                },
                [
                    "In field scope invalid scope name 'idscope'. Expected all, datasetScope or idScope",
                    "In actions[1] input should be 'READ' or 'WRITE'. Got 'OWNER'.",
                ],
                id="Wrong case for extractionPipelinesAcl idScope",
            ),
            pytest.param(
                {"appConfigAcl": {"actions": ["READ", "WRITE"], "scope": {"appScope": {"apps": ["UNKNOWN_APP"]}}}},
                ["In scope.apps[1] input should be 'SEARCH'. Got 'UNKNOWN_APP'."],
                id="Invalid app name in appConfigAcl",
            ),
            pytest.param(
                {"agentsAcl": {"actions": ["READ", "LIST"], "scope": {"idScope": {"ids": ["my_agent"]}}}},
                [
                    "In field scope invalid scope name 'idScope'. Expected all",
                    "In actions[2] input should be 'READ', 'WRITE' or 'RUN'. Got 'LIST'.",
                ],
                id="AgentsAcl with invalid scope and action",
            ),
        ],
    )
    def test_invalid_capability(self, data: dict[str, object], expected_errors: list[str]) -> None:
        warnings = validate_resource_yaml_pydantic(data, Capability, source_file=Path("filename.yaml"))
        assert len(warnings) == 1
        warning = warnings[0]
        assert isinstance(warning, ResourceFormatWarning)
        assert list(warning.errors) == expected_errors
