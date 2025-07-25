from collections.abc import Iterable
from unittest.mock import MagicMock

import pytest
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import ClientCredentials, OidcCredentials

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.data_classes.raw import RawTable
from cognite_toolkit._cdf_tk.exceptions import ToolkitError, ToolkitRequiredValueError, ToolkitTypeError
from cognite_toolkit._cdf_tk.utils.cdf import (
    get_transformation_destination_columns,
    get_transformation_sources,
    read_auth,
    try_find_error,
)


class TestTryFindError:
    @pytest.mark.parametrize(
        "credentials, expected",
        [
            pytest.param(
                ClientCredentials("${ENVIRONMENT_VAR}", "1234"),
                "The environment variable is not set: ENVIRONMENT_VAR.",
                id="Missing environment variable",
            ),
            pytest.param(
                ClientCredentials("${ENV1}", "${ENV2}"),
                "The environment variables are not set: ENV1 and ENV2.",
                id="Missing environment variable",
            ),
            pytest.param(
                OidcCredentials(
                    client_id="my-client-id",
                    client_secret="123",
                    scopes=["https://cognite.com"],
                    token_uri="not-valid-uri",
                    cdf_project_name="my-project",
                ),
                "The tokenUri 'not-valid-uri' is not a valid URI.",
            ),
            pytest.param(None, None, id="empty"),
        ],
    )
    def test_try_find_error(self, credentials: OidcCredentials | ClientCredentials | None, expected: str | None):
        assert try_find_error(credentials) == expected


def get_transformation_source_test_cases() -> Iterable:
    yield pytest.param(
        """
select
  identifier as externalId,
  name as name
from
    my_db.my_table""",
        [RawTable(db_name="my_db", table_name="my_table")],
        ["externalId", "name"],
        id="Simple query without joins",
    )

    yield pytest.param(
        """
        select table.identifier as externalId,
               table.name       as name
        from `my db`.`my table` as table""",
        [RawTable(db_name="my db", table_name="my table")],
        ["externalId", "name"],
        id="Use of backticks in table name with AS alias",
    )

    yield pytest.param(
        """SELECT
    table1.externalId as externalId,
    table2.name as name
FROM
    `ingestion`.`dump` as table1
LEFT JOIN
    `ingestion`.`workitem` as table2
ON table1.shared_column = table2.shared_column""",
        [RawTable(db_name="ingestion", table_name="dump"), RawTable(db_name="ingestion", table_name="workitem")],
        ["externalId", "name"],
        id="Query with left join",
    )

    yield pytest.param(
        """with parentLookup as (
  select
    concat('WMT:', cast(d1.`WMT_TAG_NAME` as STRING)) as externalId,
    node_reference('springfield_instances',  concat('WMT:', cast(d2.`WMT_TAG_NAME` as STRING))) as parent
  from
      `ingestion`.`dump` as  d1
  join
    `ingestion`.`dump` as d2
  on
    d1.`WMT_TAG_ID_ANCESTOR` = d2.`WMT_TAG_ID`
  where
    isnotnull(d1.`WMT_TAG_NAME`) AND
    cast(d1.`WMT_CATEGORY_ID` as INT) = 1157 AND
    isnotnull(d2.`WMT_TAG_NAME`) AND
    cast(d2.`WMT_CATEGORY_ID` as INT) = 1157
)
select
	concat('WMT:', cast(d3.`WMT_TAG_NAME` as STRING)) as externalId,
    parentLookup.parent,
    cast(`WMT_TAG_NAME` as STRING) as name,
    cast(`WMT_TAG_DESC` as STRING) as description,
    cast(`WMT_TAG_ID` as STRING) as sourceId,
    cast(`WMT_TAG_CREATED_DATE` as TIMESTAMP) as sourceCreatedTime,
    cast(`WMT_TAG_UPDATED_DATE` as TIMESTAMP) as sourceUpdatedTime,
    cast(`WMT_TAG_UPDATED_BY` as STRING) as sourceUpdatedUser
from
  `ingestion`.`dump` as d3
left join
	parentLookup
on
 concat('WMT:', cast(d3.`WMT_TAG_NAME` as STRING)) = parentLookup.externalId
where
  isnotnull(d3.`WMT_TAG_NAME`) AND
/* Inspection of the WMT_TAG_DESC looks like asset are category 1157 while equipment is everything else */
  cast(d3.`WMT_CATEGORY_ID` as INT) = 1157""",
        [RawTable(db_name="ingestion", table_name="dump")],
        [
            "externalId",
            "parent",
            "name",
            "description",
            "sourceId",
            "sourceCreatedTime",
            "sourceUpdatedTime",
            "sourceUpdatedUser",
        ],
        id="Nested query with join",
    )
    yield pytest.param(
        """select
  a.FL_0FUNCT_LOC as externalId,
  a.FL_0COSTCENTER as description,
  to_metadata(
    a.FL_0ABCINDIC,
    a.FL_0BEGRU,
    a.FL_0COMP_CODE,
  ) as metadata
from
  DTN.SAP_Functional_Location as a
  inner join (
    select
      *
    from
      DTN.SAP_Functional_Location
    where
      FL_ZFLOC_PAR NOT LIKE "ZZ%"
  ) as v on a.FL_ZFLOC_PAR = v.FL_0FUNCT_LOC
where
  is_new('my_sap_2123,', a.lastUpdatedTime)
  AND size(split(a.FL_ZFLOC_PAR, "-")) > 1
  AND a.FL_ZFLOC_PAR NOT LIKE "ZZ%"
  AND a.FL_0FUNCT_LOC NOT LIKE "TG-TGP-18%"
""",
        [RawTable(db_name="DTN", table_name="SAP_Functional_Location")],
        ["externalId", "description", "metadata"],
        id="Query with inner join",
    )

    yield pytest.param(
        """with
  asset_metadata_to_add as
    (select
        Vulnerability
  from `UpdateDB`.`Vulnerability`),

  exisiting_metadata as
    (select metadata, externalId from _cdf.assets where externalId in (select Floc from asset_metadata_to_add)),

  combined_table as
    (select asset_metadata_to_add.*,
        exisiting_metadata.*
      from asset_metadata_to_add
      JOIN exisiting_metadata
      on asset_metadata_to_add.Floc = exisiting_metadata.externalId)

select
  map_concat(to_metadata(
        Vulnerability),
        metadata) as metadata,
  combined_table.externalId as externalId
  from
  combined_table""",
        [RawTable(db_name="UpdateDB", table_name="Vulnerability"), "assets"],
        ["metadata", "externalId"],
        id="Source inside inner With Query",
    )

    yield pytest.param(
        """select
id as id,
externalId as externalId,
name as name,
1234 as dataSetId
from _cdf.assets
where left(externalId,5)="GM-ST" and left(name,2)="ST"
    """,
        ["assets"],
        ["id", "externalId", "name", "dataSetId"],
        id="Source is _cdf.assets (not a RawTable)",
    )
    yield pytest.param(
        """with
  created_assets as (
    select
      collect_set(externalId) as assets
    from
      _cdf.assets
    where
      dataSetId = 5238979812378
  ),
  bad_vals as (
    select
      array(
        '000000000012345678',
      ) as bv
  ),
  filtered_equipment as (
    select
      EQ_0EQUIPMENT as externalId
    from
      DTN.`SAP_EQUIPMENT`
  )
select
  *
from
  filtered_equipment
where
EQ_0FUNCT_LOC in (
    select
      explode(assets)
    from
      created_assets)
AND EQ_0EQUIPMENT not in (
  select
    explode(bv)
  from
    bad_vals
)
AND EQ_ZEQUI_PAR not in (
  select
    explode(bv)
  from
    bad_vals
)
""",
        ["assets", RawTable(db_name="DTN", table_name="SAP_EQUIPMENT")],
        ["*"],
        id="Source is a RawTable and _cdf.assets in a With Query",
    )

    yield pytest.param(
        """SELECT
  CASE
    WHEN REPLACE(LMD_ID, LMD.Component_ID, '') != LMD_ID THEN CONCAT(REPLACE(CP.parentExternalId, LMD.Component_ID, ''), LMD_ID)
    ELSE CONCAT(REPLACE(TRIM(CT.INT_CTRL_NO1), LMD.Circuit_ID, ''), LMD_ID)
  END AS externalId
  ,CASE
    WHEN LMD.Component_ID IS NULL THEN TRIM(CT.INT_CTRL_NO1)
    ELSE CP.parentExternalId
  END AS parentExternalId
  ,CASE
    WHEN LMD.Component_ID IS NULL THEN REPLACE(LMD_ID, CONCAT(LMD.Circuit_ID, '-'), '')
    ELSE REPLACE(LMD_ID, CONCAT(LMD.Component_ID, '-'), '')
  END AS name
  ,LMD.description AS description
  ,dataset_id('PSSD') AS dataSetId
  ,to_metadata_except(array('description', 'FACILITY'), LMD.*) AS metadata
  ,array('LMD') as labels
FROM (
  SELECT
    TRIM(EQUIP_ID) AS Equipment_ID
  FROM PSSD.PSSD_LMD
  WHERE is_new('pssd_lmd', lastUpdatedTime)) LMD
LEFT JOIN (
  SELECT
    CASE
      WHEN TRIM(`Internal Control No 1`) LIKE "%_E" THEN CONCAT(REPLACE(TRIM(ASSET_NO), TRIM(`EQUIPMENT ID`), ''), REPLACE(TRIM(`component_id`), ' ', ''))
      ELSE CONCAT(REPLACE(CL.INT_CTRL_NO1, TRIM(CIRCUIT_ID), ''), REPLACE(TRIM(`component_id`), ' ', ''))
    END AS parentExternalId,
    REPLACE(TRIM(`component_id`), ' ', '') AS  component_id,
    TRIM(`FACILITY ID`) AS FACILITY,
    TRIM(CPT.`EQUIPMENT ID`) AS EQUIP_ID
  FROM PSSD.PSSD_component CPT
  LEFT JOIN PSSD.PSSD_circuit CL
    ON TRIM(`Internal Control No 1`) = CL.CIRCUIT_ID
    AND TRIM(`FACILITY ID`) = FACILITY
    AND TRIM(CPT.`EQUIPMENT ID`) = TRIM(CL.EQUIP_ID)
  LEFT JOIN (
    SELECT externalId AS cdf_asset
    FROM _cdf.assets) CDF
      ON CASE
        WHEN TRIM(`Internal Control No 1`) LIKE "%_E" THEN TRIM(ASSET_NO)
        ELSE CL.INT_CTRL_NO1
      END = CDF.cdf_asset
  WHERE (TRIM(`Internal Control No 1`) LIKE "%_E"
    OR CIRCUIT_ID IS NOT NULL)
    AND cdf_asset IS NOT NULL
  ) CP
  ON TRIM(LMD.Component_ID) = TRIM(CP.component_id)
    AND TRIM(LMD.FACILITY) = TRIM(CP.FACILITY)
    AND LMD.Equipment_ID = CP.EQUIP_ID
    AND LMD.Circuit_ID = CP.CIRCUIT_ID
LEFT JOIN PSSD.PSSD_circuit CT
  ON TRIM(LMD.Circuit_ID) = TRIM(CT.CIRCUIT_ID)
  AND TRIM(LMD.FACILITY) = TRIM(CT.FACILITY)
  AND TRIM(LMD.Equipment_ID) = TRIM(CT.EQUIP_ID)
LEFT JOIN (
  SELECT externalId AS cdf_asset
  FROM _cdf.assets) CDF2
  ON CASE
    WHEN LMD.Component_ID IS NULL THEN TRIM(CT.INT_CTRL_NO1)
    ELSE CP.parentExternalId
  END = CDF2.cdf_asset
WHERE CP.component_id IS NOT NULL
  OR (CT.CIRCUIT_ID IS NOT NULL
    AND LMD.Component_ID IS NULL)
    AND cdf_asset IS NOT NULL
""",
        [RawTable("PSSD", "PSSD_LMD"), RawTable("PSSD", "PSSD_component"), RawTable("PSSD", "PSSD_circuit"), "assets"],
        ["externalId", "parentExternalId", "name", "description", "dataSetId", "metadata", "labels"],
        id="Complex query with multiple joins and CDF assets",
    )

    yield pytest.param(
        """SELECT
  id,
  map_concat(
    metadata,
    to_metadata(
        Station_Name,
        WellName,
        GIS_latitude,
        GIS_longitude,
        SPSOnLine,
        idwell
    )
  ) as metadata
FROM
  (
    select
      a.id,
      ws.Station_Name,
      sdo.WellName,
      gis.WI_LATITUDE as GIS_latitude,
      gis.WI_LONGITUDE as GIS_longitude,
      pw.SPSOnLine,
      wh.idwell
    FROM
      _cdf.assets as a
      left join SDO.`well_header` as sdo on a.metadata['PRANumber6Digits'] = sdo.PraNumber
      left join SDO.`pw_DepletionPlan` AS pw on a.metadata['PRANumber6Digits'] = pw.PRANumber6Digits
      left join SDO.`well_header2` as wh on substring(sdo.ApiNumber, 0, 10) = substring(wellida, 0, 10)
      left join `GIS`.`gis` as gis on substring(sdo.ApiNumber, 0, 10) = substring(WI_APINO, 0, 10)
      left join SDO.`weather_station` as ws on sdo.ApiNumber = ws.ApiNumber
    where
      array_contains(labels, "my_location_sap")
  )
    """,
        [
            "assets",
            RawTable(db_name="SDO", table_name="well_header"),
            RawTable(db_name="SDO", table_name="pw_DepletionPlan"),
            RawTable(db_name="SDO", table_name="well_header2"),
            RawTable(db_name="GIS", table_name="gis"),
            RawTable(db_name="SDO", table_name="weather_station"),
        ],
        ["id", "metadata"],
        id="Complex query with multiple joins and CDF assets with metadata",
    )

    yield pytest.param(
        """SELECT
  cast(concat("GM", `Tag name`) as STRING) as externalId,
  cast(`Parent tag` as STRING) as parentExternalId,
  cast(concat("TB-",`Tag name`) as STRING) as name,
  cast(concat(`Tag class name`," ", `Tag description`) as STRING) as description,
  1238456789 as dataSetId,
  'Engineering Documents' as source
FROM `GM_DATA`.`verified_new_only`

WHERE concat("GM", `Tag name`) IN (

    WITH existing_ext_ids AS (
        SELECT DISTINCT externalId AS value
        FROM _cdf.assets
    ),
    new_ext_ids AS (
        SELECT DISTINCT concat("GM", `Tag name`) AS value
        FROM `GM_DATA`.`verified_new_only`
    )
    SELECT COALESCE(existing_ext_ids.value, new_ext_ids.value) AS differing_value
    FROM existing_ext_ids
    RIGHT JOIN new_ext_ids
    ON existing_ext_ids.value = new_ext_ids.value
    WHERE existing_ext_ids.value IS NULL OR new_ext_ids.value IS NULL
    )
;""",
        [RawTable(db_name="GM_DATA", table_name="verified_new_only"), "assets"],
        ["externalId", "parentExternalId", "name", "description", "dataSetId", "source"],
        id="Query listed in WHERE clause",
    )

    yield pytest.param(
        """SELECT asset.id as id,
        array("SAP DELETED") as labels
        -- if(asset.labels is null, array("SAP DELETED"), array_union(asset.labels, array("SAP DELETED"))) as labels
FROM
(SELECT
CONCAT(REPLACE(EQ_0FUNCT_LOC, "-IN", ""), "-", key) AS ext_id
FROM DVT.`SAP_Equipment`
WHERE RLIKE(EQ_0MD,"FL")
UNION
SELECT
FL_0FUNCT_LOC AS ext_id
FROM DVT.SAP_Functional_Location
WHERE FL_PAR LIKE "ZZ%"
OR FL_SYS_ST != "CE"
UNION
SELECT
  FIRST(externalId) as ext_id
FROM (
  SELECT *,
   element_at(split(externalId, "-"), -1) as equip_id,
    COUNT(*) OVER (PARTITION BY element_at(split(externalId, "-"), -1)) AS count
  FROM `_cdf`.`assets`
  where dataSetId = dataset_id("sap_equipment")
  or dataSetId = dataset_id("midstream_equipment")
  order by createdTime asc
) tmp
WHERE count > 1
GROUP BY equip_id
)

INNER JOIN _cdf.assets asset on ext_id = asset.externalId
""",
        [
            RawTable(db_name="DVT", table_name="SAP_Equipment"),
            RawTable(db_name="DVT", table_name="SAP_Functional_Location"),
            "assets",
        ],
        ["id", "labels"],
        id="Query with UNION and multiple sources including CDF assets and a comment in the Select clause",
    )

    yield pytest.param(
        """
        select identifier as externalId,
            /* This is a comment */
               name       as name
        from my_db.my_table""",
        [RawTable(db_name="my_db", table_name="my_table")],
        ["externalId", "name"],
        id="Simple query with comment",
    )

    yield pytest.param(
        """with assetid (
select
  first(wa.sourceExternalId) as workorderid,
  collect_list(ai.id)        as assetIds
from
  `workorder_oid_workmate`.`workorder2assets` wa,
  _cdf.assets                                 ai
where
  ai.externalId = wa.targetExternalId
GROUP BY sourceExternalId)
select
  wo.externalId,
  wo.description,
  dataset_id('ds_transformations_oid')  as dataSetId,
  cast(from_unixtime(double(wo.`startTime`)/1000)
                         as TIMESTAMP)  as startTime,
  cast(from_unixtime(double(wo.`endTime`)/1000)
                         as TIMESTAMP)  as endTime,
  ai.assetIds                           as assetIds,
  "workorder"                           as type,
  "OID - workmate"                      as source,
  to_metadata_except(
    array("key",
          "startTime",
          "endTime",
          "externalId",
          "wo.description",
          "assetIds"), *)              as metadata
from
  `workorder_oid_workmate`.`workorders` wo,
  assetid ai
where
  ai.workorderid = wo.workOrderNumber
  and wo.`endTime` > wo.`startTime`""",
        [
            RawTable(db_name="workorder_oid_workmate", table_name="workorder2assets"),
            "assets",
            RawTable(db_name="workorder_oid_workmate", table_name="workorders"),
        ],
        [
            "externalId",
            "description",
            "dataSetId",
            "startTime",
            "endTime",
            "assetIds",
            "type",
            "source",
            "metadata",
        ],
        id="Query with listed from.",
    )


class TestGetTransformationSource:
    @pytest.mark.parametrize(
        "query, expected_sources, expected_destination_columns", list(get_transformation_source_test_cases())
    )
    def test_get_transformation_sources(
        self, query: str, expected_sources: list[RawTable | str], expected_destination_columns: list[str]
    ) -> None:
        """Test that the transformation source is correctly extracted from the query."""
        actual = get_transformation_sources(query)
        assert actual == expected_sources


class TestGetTransformationDestinationColumns:
    @pytest.mark.parametrize(
        "query, expected_sources, expected_destination_columns",
        list(get_transformation_source_test_cases()),
    )
    def test_get_transformation_sources_and_destination_columns(
        self, query: str, expected_sources: list[RawTable | str], expected_destination_columns: list[str]
    ) -> None:
        """Test that the transformation source and destination columns are correctly extracted from the query."""
        actual = get_transformation_destination_columns(query)
        assert actual == expected_destination_columns


class TestReadAuth:
    @pytest.mark.parametrize(
        "auth, expected",
        [
            pytest.param(
                {
                    "clientId": "my_id",
                    "clientSecret": "my_secret",
                },
                ClientCredentials("my_id", "my_secret"),
                id="Client credentials",
            ),
            pytest.param(
                {
                    "clientId": "my_id",
                    "clientSecret": "my_secret",
                    "tokenUri": "https://my-token-uri",
                    "cdfProjectName": "my-project",
                },
                OidcCredentials("my_id", "my_secret", "https://my-token-uri", "my-project"),
                id="OIDC credentials only required",
            ),
            pytest.param(
                {
                    "clientId": "my_id",
                    "clientSecret": "my_secret",
                    "tokenUri": "https://my-token-uri",
                    "cdfProjectName": "my-project",
                    "scopes": "USER_IMPERSONATION,https://cognite.com",
                    "audience": "https://cognite.com",
                },
                OidcCredentials(
                    "my_id",
                    "my_secret",
                    "https://my-token-uri",
                    "my-project",
                    ["USER_IMPERSONATION", "https://cognite.com"],
                    "https://cognite.com",
                ),
                id="OIDC credentials all fields",
            ),
        ],
    )
    def test_read_valid_auth(self, auth: object, expected: ClientCredentials | OidcCredentials) -> None:
        config = MagicMock(spec=ToolkitClientConfig)
        result = read_auth(auth, config, "only-used-in-errors", "only-used-in-errors", allow_oidc=True)
        assert isinstance(result, ClientCredentials | OidcCredentials)
        assert result.dump() == expected.dump()

    @pytest.mark.parametrize(
        "auth, expected_exception",
        [
            pytest.param(
                None,
                ToolkitRequiredValueError("Authentication is missing for compute resource 'my_compute_resource'."),
                id="Missing authentication",
            ),
            pytest.param(
                123,
                ToolkitTypeError("Authentication must be a dictionary for compute resource 'my_compute_resource'"),
            ),
            pytest.param(
                {"clientId": "my_id"},
                ToolkitRequiredValueError(
                    "Authentication must contain clientId and clientSecret for compute resource 'my_compute_resource'"
                ),
            ),
        ],
    )
    def test_read_invalid_auth(self, auth: object, expected_exception: ToolkitError) -> None:
        with pytest.raises(type(expected_exception)) as excinfo:
            config = MagicMock(spec=ToolkitClientConfig)
            config.is_strict_validation = True
            read_auth(auth, config, "my_compute_resource", "compute resource")

        assert str(excinfo.value) == str(expected_exception)

    def test_read_warning_auth(self) -> None:
        credentials = OAuthClientCredentials("url", "my_id", "my_secret", ["USER_IMPERSONATION"])
        config = MagicMock(spec=ToolkitClientConfig)
        config.is_strict_validation = False
        config.credentials = credentials
        warning: str = ""

        def catch_warning_message(*messages: object) -> None:
            nonlocal warning
            warning = "".join(map(str, messages))

        console = MagicMock()
        console.print = catch_warning_message

        result = read_auth(None, config, "my_compute_resource", "compute resource", console=console)
        assert (
            "Authentication is missing for compute resource 'my_compute_resource'. "
            "Falling back to the Toolkit credentials"
        ) in warning
        assert isinstance(result, ClientCredentials)
        assert result.dump() == {
            "clientId": "my_id",
            "clientSecret": "my_secret",
        }
