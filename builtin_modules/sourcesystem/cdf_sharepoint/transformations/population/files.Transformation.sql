select
	concat('VAL_', name) as externalId,
 	name,
	source as sourceId,
	mime_type as mimeType
from `{{ rawSourceDatabase }}`.`files_metadata`
where
	isnotnull(mime_type) and
	mime_type = 'application/pdf'
