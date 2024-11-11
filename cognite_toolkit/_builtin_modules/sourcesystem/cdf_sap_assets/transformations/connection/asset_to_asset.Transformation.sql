select
  concat('WMT:', cast(d1.`WMT_TAG_NAME` as STRING)) as externalId,
  node_reference('{{ instanceSpace }}',  concat('WMT:', cast(d2.`WMT_TAG_NAME` as STRING))) as parent
from
    {{ rawSourceDatabase }}.`dump` d1
join
  {{ rawSourceDatabase }}.`dump` d2
on
  d1.`WMT_TAG_ID_ANCESTOR` = d2.`WMT_TAG_ID`
where
  isnotnull(d1.`WMT_TAG_NAME`) AND
  cast(d1.`WMT_CATEGORY_ID` as INT) = 1157 AND
  isnotnull(d2.`WMT_TAG_NAME`) AND
/* Inspection of the WMT_TAG_DESC looks like asset are category 1157 while equipment is everything else */
  cast(d2.`WMT_CATEGORY_ID` as INT) = 1157
