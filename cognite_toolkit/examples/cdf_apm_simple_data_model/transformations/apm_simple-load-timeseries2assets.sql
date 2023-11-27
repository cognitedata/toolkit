select
  cast(`asset` as STRING) as externalId,
  array(timeseries) as metrics
from
  `{{source_raw_db}}`.`timeseries2assets`;
