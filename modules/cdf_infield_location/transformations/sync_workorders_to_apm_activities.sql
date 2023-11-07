  select
    cast(`externalId` as STRING) as externalId,
    cast(`description` as STRING) as description,
    cast(`key` as STRING) as id,
    cast(`status` as STRING) as status,
    /* cast(`startTime` as TIMESTAMP) as startTime,
    cast(`endTime` as TIMESTAMP) as endTime,*/
    cast('2023-11-06T09:00:00' as TIMESTAMP) as startTime,
    cast('2023-11-10T09:00:00' as TIMESTAMP) as endTime,
    cast(`title` as STRING) as title,
    '{{root_asset_external_id}}' as rootLocation,
    'workmate' as source
  from
    `{{raw_db}}`.`{{workorder_table_name}}`;
