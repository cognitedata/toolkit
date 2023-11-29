select
  cast(`asset` as STRING) as externalId,
  array(timeseries) as metrics
from
  `{{files_raw_db}}`.`timeseries2assets`;
