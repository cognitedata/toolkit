from datetime import datetime
from typing import Optional
from pydantic import BaseModel
from cognite.client import CogniteClient

class TaskBaseModel(BaseModel):
    client: CogniteClient
    
    