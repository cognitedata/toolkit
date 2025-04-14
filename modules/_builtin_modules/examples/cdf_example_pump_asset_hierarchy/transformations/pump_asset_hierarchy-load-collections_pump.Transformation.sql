--- 1. asset root (defining all columns)
SELECT
    "Lift Pump Stations" AS name,
    dataset_id("{{data_set}}") AS dataSetId,
    "lift_pump_stations:root" AS externalId,
    '' as parentExternalId,
    "An example pump dataset" as description,
    null as metadata

UNION ALL
--- 2. Lift Stations
select
    s.lift_station as name,
    dataset_id("{{data_set}}") AS dataSetId,
    concat("lift_station:", lower(replace(s.lift_station, ' ', '_'))) as externalId,
    'lift_pump_stations:root' as parentExternalId,
    null as description,
    null as metadata
FROM (
    select
        first_value(LiftStationID) as lift_station
    from {{raw_db}}.`collections_pump`
    group by LiftStationID
) as s

UNION ALL
--- 3. Pumps
SELECT
    concat("Pump ", PumpModel) as name,
    dataset_id("{{data_set}}") AS dataSetId,
    GlobalID as externalId,
    concat("lift_station:", lower(replace(LiftStationID, ' ', '_'))) as parentExternalId,
    Comments as description,
    to_metadata(
  PumpOn,
  PumpOff,
  VFD,
  VFDSetting,
  Position,
  LiftStationID,
  PumpNumber,
  PumpHP,
  HighHeadShutOff,
  DesignPointHeadFT,
  DesignPointFlowGPM,
  LowHeadFT,
  LowHeadFlowGPM,
  PumpControl,
  PumpModel,
  Shape__Length,
  Enabled,
  DesignPointHeadFT,
  LowHeadFT,
  FacilityID,
  InstallDate,
  LifeCycleStatus,
  LocationDescription
  ) as metadata
from `{{raw_db}}`.`collections_pump`
