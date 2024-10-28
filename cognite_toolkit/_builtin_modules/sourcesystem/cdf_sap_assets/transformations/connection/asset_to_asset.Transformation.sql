select
  concat('WMT:', cast(d1.`WMT_TAG_NAME` as STRING)) as externalId
from
  `{{ rawDatabase }}`.`dump`
where
  isnotnull(`WMT_TAG_NAME`) AND
/* Inspection of the WMT_TAG_DESC looks like asset are category 1157 while equipment is everything else */
  cast(`WMT_CATEGORY_ID` as INT) = 1157
