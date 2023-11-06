  select
    cast(`externalId` as STRING) as externalId,
    cast(`description` as STRING) as description,
    cast(`endTime` as TIMESTAMP) as endTime,
    cast(`key` as STRING) as id,
    cast(`status` as STRING) as status,
    cast(`startTime` as TIMESTAMP) as startTime,
    cast(`title` as STRING) as title
  from
    `{{raw_db}}`.`{{workorder_table_name}}`;
