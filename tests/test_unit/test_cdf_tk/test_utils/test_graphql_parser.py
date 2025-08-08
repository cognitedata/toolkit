import pytest
from cognite.client.data_classes.data_modeling import DataModelId, ViewId

from cognite_toolkit._cdf_tk.utils import (
    GraphQLParser,
)
from cognite_toolkit._cdf_tk.utils.graphql_parser import _Directive, _DirectiveTokens, _ViewDirective

SPACE = "sp_my_space"
DATA_MODEL = DataModelId(SPACE, "MyDataModel", "v1")
GraphQLTestCases = [
    pytest.param(
        '''"Log File"
type LogFile
  @view(
    rawFilter: {
      hasData: [
        {
          type: "container"
          space: "onse_logfile"
          externalId: "LogFile"
        }
      ]
    }  version: "1"
  ) {
  """
  Name of the CSV which delivers result/logs from ONSE
  @name CSV LogFile Name
  """
  csv_logfile_name: String
  """
  CDF address of the posted CSV log file from ONSE
  @name CSV LogFile Unique ID
  """
  csv_logfile_uniqueId: Float
  """
  General result from ONSE such as Success or Partial or Failure (Status)
  @name Status
  """
  status: String
  """
  Cryptic id from IX representing the Transmittal/transaction (key)
  @name Transaction id
  """
  transaction_id: String
  """
  Transmittal Number (Referemce/Identifier)
  @name Transmittal Number
  """
  transmittalNumber: String
  """
  Date and time when xml LogFile has been posted to CDF
  @name Uploaded to CDF  Time
  """
  uploaded_to_cdf: String
  """
  Name of the XML which delivers result/logs from ONSE
  @name XML LogFile Name
  """
  xml_logfile_name: String
  """
  CDF address of the posted XML log file from ONSE
  @name XML LogFile Unique ID
  """
  xml_logfile_uniqueId: Float
}

"Log File From ONSE"
type LogFile_DryRun
  @view(
    rawFilter: {
      hasData: [
        {
          type: "container"
          space: "onse_logfile"
          externalId: "LogFile_DryRun"
        }
      ]
    }  version: "1"
  ) {
  """
  Name of the CSV which delivers result/logs from ONSE
  @name CSV LogFile Name
  """
  csv_logfile_name: String
  """
  CDF address of the posted CSV log file from ONSE
  @name CSV LogFile Unique ID
  """
  csv_logfile_uniqueId: Float
  """
  General result from ONSE
  @name Status
  """
  status: String
  """
  Cryptic id from IX
  @name Transaction id
  """
  transaction_id: String
  """
  Transmittal Number (Referemce/Identifier)
  @name Transmittal Number
  """
  transmittalNumber: String
  """
  Date and time when xml LogFile has been posted to CDF
  @name Uploaded to CDF  Time
  """
  uploaded_to_cdf: String
  """
  Name of the XML which delivers result/logs from ONSE
  @name XML LogFile Name
  """
  xml_logfile_name: String
  """
  CDF address of the posted XML log file from ONSE
  @name XML LogFile Unique ID
  """
  xml_logfile_uniqueId: Float
}
''',
        DATA_MODEL,
        {ViewId(SPACE, "LogFile", "1"), ViewId(SPACE, "LogFile_DryRun", "1")},
        set(),
        id="Simple type with view and raw filter",
    ),
    pytest.param(
        """interface Creatable @view(space: "cdf_apps_shared", version: "v1") @import {
  visibility: String
  createdBy: CDF_User
  updatedBy: CDF_User
  isArchived: Boolean
}""",
        DATA_MODEL,
        set(),
        {ViewId("cdf_apps_shared", "Creatable", "v1")},
        id="Imported interface",
    ),
    pytest.param(
        """"@code WOOL"
type WorkOrderObjectListItem @import(dataModel: {externalId: "MaintenanceDOM", version: "2_2_0", space: "EDG-COR-ALL-DMD"}) {
  ...
}""",
        DATA_MODEL,
        set(),
        {DataModelId(space="EDG-COR-ALL-DMD", external_id="MaintenanceDOM", version="2_2_0")},
        id="Imported data model",
    ),
    pytest.param(
        '''
"""
@name        Notification
@description This is a SAP notification object.
@code        NOTI
"""
type Notification @container(indexes: [{identifier: "refCostCenterIndex", indexType: BTREE, fields: ["refCostCenter"]}, {identifier: "createdDateIndex", indexType: BTREE, fields: ["createdDate"], cursorable: true}]) @import(dataModel: {externalId: "MaintenanceDOM", version: "2_2_0", space: "EDG-COR-ALL-DMD"}) {
  ...
  }
''',
        DATA_MODEL,
        set(),
        {DataModelId(space="EDG-COR-ALL-DMD", external_id="MaintenanceDOM", version="2_2_0")},
        id="Imported data model with unused container directive",
    ),
    pytest.param(
        '''
    """
@code WKCC
@Description Work Center Category in SAP
"""
type WorkCenterCategory {
    """
    @name Name
    """
    name: String!
    """
    @name Description
    """
    description: String
    """
    @name Code
    """
    code: String
}''',
        DATA_MODEL,
        {ViewId(SPACE, "WorkCenterCategory", None)},
        set(),
        id="Simple type",
    ),
    pytest.param(
        """type Cdf3dConnectionProperties
  @import
  @view(space: "cdf_3d_schema", version: "1")
  @edge
  @container(
    constraints: [
      {
        identifier: "uniqueNodeRevisionConstraint"
        constraintType: UNIQUENESS
        fields: ["revisionId", "revisionNodeId"]
      }
    ]
  ) {
  revisionId: Int64!
  revisionNodeId: Int64!
}""",
        DATA_MODEL,
        set(),
        {ViewId("cdf_3d_schema", "Cdf3dConnectionProperties", "1")},
        id="Edge type",
    ),
    pytest.param(
        """type APM_User @view (version: "7") {
  name: String
  email: String
  lastSeen: Timestamp
  preferences: JSONObject
}""",
        DATA_MODEL,
        {ViewId(SPACE, "APM_User", "7")},
        set(),
        id="Simple type with version",
    ),
    pytest.param(
        """type UnitOfMeasurement
  @import(
    dataModel: {
      externalId: "CoreDOM"
      version: "1_0_18"
      space: "EDG-COR-ALL-DMD"
    }
  ) {
  name: String!
}""",
        DATA_MODEL,
        set(),
        {DataModelId(space="EDG-COR-ALL-DMD", external_id="CoreDOM", version="1_0_18")},
        id="No comma, only newline",
    ),
    pytest.param(
        '''"""
  @name Name }{ Breaks the parser
"""
type UnitOfMeasurement
  @import(
    dataModel: {
      externalId: "CoreDOM"
      version: "1_0_18"
      space: "EDG-COR-ALL-DMD"
    }
  ) {
  name: String!
}''',
        DATA_MODEL,
        set(),
        {DataModelId(space="EDG-COR-ALL-DMD", external_id="CoreDOM", version="1_0_18")},
        id="Ignore comments",
    ),
    pytest.param(
        """type APM_Config @view {
  name: String
  appDataSpaceId: String
  appDataSpaceVersion: String
  customerDataSpaceId: String
  customerDataSpaceVersion: String
  featureConfiguration: JSONObject
  fieldConfiguration: JSONObject
  rootLocationsConfiguration: JSONObject
}""",
        DATA_MODEL,
        {ViewId(SPACE, "APM_Config", None)},
        set(),
        id="No version",
    ),
    pytest.param(
        """"Navigational aid for traversing CogniteCADModel instances"
type CogniteCADModel implements CogniteDescribable & Cognite3DModel
  @view(
    space: "cdf_cdm"
    version: "v1"
    rawFilter: {
      and: [
        {
          hasData: [
            {
              type: "container"
              space: "cdf_cdm_3d"
              externalId: "Cognite3DModel"
            }
          ]
        }
        {
          equals: {
            property: ["cdf_cdm_3d", "Cognite3DModel", "type"]
            value: "CAD"
          }
        }
      ]
    }
  )
  @import {
  "Name of the instance"
  name: String
  "Description of the instance"
  description: String
  "Text based labels for generic use, limited to 1000"
  tags: [String]
  "Alternative names for the node"
  aliases: [String]
  "CAD, PointCloud or Image360"
  type: Cognite3DModel_type
  "Thumbnail of the 3D model"
  thumbnail: CogniteFile
  "List of revisions for this CAD model"
  revisions: [CogniteCADRevision]
    @reverseDirectRelation(throughProperty: "model3D")
}""",
        DATA_MODEL,
        set(),
        {ViewId("cdf_cdm", "CogniteCADModel", "v1")},
        id="Setting custom filter on view",
    ),
    pytest.param(
        '''"""
@name Tag (Beta)
@code CTG
@Description Beta version only. Should not be used unless aligned with Celanese Data Governance Owner. Tag is an object designed for performing functional requirements and serving as a specification for equipment.
"""
type TagBeta @view (version: "7#") {
  name: String
  description: String
  aliases: [String]
  isActive: Boolean
  tagTypes: [TagType]
  tagClass: CfihosTagClass
  functionalLocation: FunctionalLocation # --> To be deprecated. Use functionalLocations instead
  functionalLocations: [FunctionalLocation]
  equipment: Equipment # --> To be deprecated. Use equipments instead
  equipments: [Equipment]
  reportingUnit: ReportingUnit # --> To be deprecated. Use reportingUnits instead
  reportingUnits: [ReportingUnit]
}''',
        DATA_MODEL,
        {ViewId(SPACE, "TagBeta", "7#")},
        set(),
        id="Type with comments",
    ),
    pytest.param(
        """type Alarm @view (rawFilter:
{equals : {property: ["PSI-COR-ALL-DMD", "AlarmEventMessage", "journal"], value: "Alarm"}}
)
{
  journal:String @mapping(container: "AlarmEventMessage")
  dataOwner:AlarmEventMessageDataOwner @mapping(container: "AlarmEventMessage")
  aemTimeStamp: Timestamp @mapping(container: "AlarmEventMessage")
  pointTag:String @mapping(container: "AlarmEventMessage")
  pointTagDesc:String @mapping(container: "AlarmEventMessage")
  psiUnit:String @mapping(container: "AlarmEventMessage")
  code:String @mapping(container: "AlarmEventMessage")
  alarmType:String @mapping(container: "AlarmEventMessage")
  alarmState:String @mapping(container: "AlarmEventMessage")
  priority:String @mapping(container: "AlarmEventMessage")
  limit:Int @mapping(container: "AlarmEventMessage")
  value:String @mapping(container: "AlarmEventMessage")
  sourceAlarm:String @mapping(container: "AlarmEventMessage")
  alarmEnableStatus:String @mapping(container: "AlarmEventMessage")
}""",
        DATA_MODEL,
        {ViewId(SPACE, "Alarm", None)},
        set(),
        id="Type with raw filter",
    ),
]

DirectiveTestCases = [
    pytest.param(
        """view(
    rawFilter: {
      hasData: [
        {
          type: "container"
          space: "onse_logfile"
          externalId: "LogFile"
        }
      ]
    }
    version: "1"
  )""",
        _ViewDirective(version="1"),
        id="View directive with raw filter and version",
    ),
    pytest.param(
        """view(
    space: "cdf_cdm"
    version: "v1"
    rawFilter: {
      and: [
        {
          hasData: [
            {
              type: "container"
              space: "cdf_cdm_3d"
              externalId: "Cognite3DModel"
            }
          ]
        }
        {
          equals: {
            property: ["cdf_cdm_3d", "Cognite3DModel", "type"]
            value: "CAD"
          }
        }
      ]
    }
  )""",
        _ViewDirective(space="cdf_cdm", version="v1"),
    ),
    pytest.param(
        """view (rawFilter:
{equals : {property: ["PSI-COR-ALL-DMD", "AlarmEventMessage", "journal"], value: "Alarm"}}
)""",
        _ViewDirective(),
    ),
]


class TestGraphQLParser:
    @pytest.mark.parametrize("raw, data_model_id, expected_views, dependencies", GraphQLTestCases)
    def test_parse(
        self, raw: str, data_model_id: DataModelId, expected_views: set[ViewId], dependencies: set[ViewId | DataModelId]
    ) -> None:
        parser = GraphQLParser(raw, data_model_id)

        actual_views = parser.get_views(include_version=True)
        assert expected_views == actual_views
        actual_dependencies = parser.get_dependencies(include_version=True)
        assert dependencies == actual_dependencies

    @pytest.mark.parametrize("string, expected", DirectiveTestCases)
    def test_create_directive(self, string: str, expected: _Directive) -> None:
        tokens = GraphQLParser._token_pattern.findall(string)
        actual = _DirectiveTokens(tokens).create()
        assert expected == actual
