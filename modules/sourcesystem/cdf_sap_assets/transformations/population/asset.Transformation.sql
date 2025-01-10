with parentLookup as (
  select
  	concat('WMT:', cast(d1.`WMT_TAG_NAME` as STRING)) as externalId,
    node_reference('{{ instanceSpace }}',  concat('WMT:', cast(d2.`WMT_TAG_NAME` as STRING))) as parent
  from
      `{{ rawSourceDatabase }}`.`dump` as  d1
  join
    `{{ rawSourceDatabase }}`.`dump` as d2
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
  `{{ rawSourceDatabase }}`.`dump` as d3
left join
	parentLookup
on
 concat('WMT:', cast(d3.`WMT_TAG_NAME` as STRING)) = parentLookup.externalId
where
  isnotnull(d3.`WMT_TAG_NAME`) AND
/* Inspection of the WMT_TAG_DESC looks like asset are category 1157 while equipment is everything else */
  cast(d3.`WMT_CATEGORY_ID` as INT) = 1157
