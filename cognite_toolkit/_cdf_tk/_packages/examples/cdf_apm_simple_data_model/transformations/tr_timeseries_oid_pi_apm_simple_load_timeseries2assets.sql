select
  cast(`asset` as STRING) as externalId,
  array(timeseries) as metrics
from
  `files_{{default_location}}_{{source_timeseries}}`.`timeseries2assets`;
