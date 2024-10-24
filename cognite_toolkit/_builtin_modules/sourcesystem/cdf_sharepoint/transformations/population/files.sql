select 
	external_id as externalId, 
 	name, 
	source as sourceId,
	mime_type as mimeType
from `{{ rawDatabase }}`.`files_metadata`