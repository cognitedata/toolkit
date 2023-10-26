select
  cast(`asset` as STRING) as externalId,
  array(timeseries) as metrics
from
  `{{raw_db}}`.`timeseries2assets`;
