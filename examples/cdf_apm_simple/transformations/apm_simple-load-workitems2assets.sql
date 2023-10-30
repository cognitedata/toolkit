/* MAPPING_MODE_ENABLED: true */ /* {"version":1,"sourceType":"raw","mappings":[ {"from":"externalId","to":"externalId","asType":"STRING"}, {"from":"sourceExternalId","to":"startNode","asType":"STRUCT<`space`:STRING, `externalId`:STRING>"}, {"from":"targetExternalId","to":"endNode","asType":"STRUCT<`space`:STRING, `externalId`:STRING>"}], "sourceLevel1":"tutorial_apm","sourceLevel2":"workitem2assets"} */ select
  cast(`externalId` as STRING) as externalId,
  node_reference('{{datamodel}}', `sourceExternalId`) as startNode,
  node_reference('{{datamodel}}', `targetExternalId`) as endNode
from
  `{{raw_db}}`.`workitem2assets`;
