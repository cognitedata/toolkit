select
  cast(`externalId` as STRING) as externalId
from
  `db_{{ example_variable }}`.`table_{{ example_variable }}`;
-- this is a comment with an ← Unicode character
