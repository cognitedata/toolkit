select 
	name as externalId,
 	name, 
	source as sourceId,
	mime_type as mimeType
from `{{ rawSourceDatabase }}`.`files_metadata`