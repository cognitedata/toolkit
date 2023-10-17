/* MAPPING_MODE_ENABLED: true */ /* {"version":1,"sourceType":"raw","mappings":[{"from":"externalId","to":"externalId","asType":"STRING"},{"from":"sourceExternalId","to":"startNode","asType":"STRUCT<`space`:STRING, `externalId`:STRING>"},{"to":"endNode","asType":"STRUCT<`space`:STRING, `externalId`:STRING>","from":"targetExternalId"}],"sourceLevel1":"tutorial_apm","sourceLevel2":"workorder2assets"} */ select
  cast(`externalId` as STRING) as externalId,
  node_reference('tutorial_apm_simple', `sourceExternalId`) as startNode,
  node_reference('tutorial_apm_simple', `targetExternalId`) as endNode
from
  `tutorial_apm`.`workorder2assets`;
