  select
    cast(`externalId` as STRING) as externalId,
    cast(`description` as STRING) as description,
    cast(`key` as STRING) as id,
    cast(`status` as STRING) as status,
    /* cast(`startTime` as TIMESTAMP) as startTime,
    cast(`endTime` as TIMESTAMP) as endTime,
    NOTE!!! The below two datas just updates all workorders to be from now 
    and into the future. This is done for the sake of the demo data.
    */
    cast(current_date() as TIMESTAMP) as startTime,
    cast(date_add(current_date(), 7) as TIMESTAMP) as endTime,
    cast(`title` as STRING) as title,
    '{{first_root_asset_external_id}}' as rootLocation,
    'workmate' as source
  from
    `{{workorder_raw_db}}`.`{{workorder_table_name}}`;
