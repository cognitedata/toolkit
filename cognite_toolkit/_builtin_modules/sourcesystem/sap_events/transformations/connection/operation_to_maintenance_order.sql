select
  concat('WMT:', cast(d1.`WMT_TAG_NAME` as STRING)) as externalId,
  node_reference('{{ instanceSpace }}',  concat('WMT', cast(d2.`WMT_TAG_NAME` as STRING))) as maintenanceOrder
from
    {{ rawDatabase }}.`workitem` d1
join
  {{ rawDatabase }}.`workorder` d2
on
  d1.`WMT_TAG_ID_ANCESTOR` = d2.`WMT_TAG_ID`
where
  isnotnull(d1.`WMT_TAG_NAME`