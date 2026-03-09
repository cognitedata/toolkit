"""First Function App — Cognite Function App.

Routes:
  POST  /process             Process incoming data
"""

import logging

from cognite.client import CogniteClient
from cognite_function_apps import (
    FunctionApp,
    create_function_service,
    create_introspection_app,
)
from pydantic import BaseModel

app = FunctionApp(title="First Function App", version="1.0.0")
introspection = create_introspection_app()


# ── Models ────────────────────────────────────────────────────────────────────


class ProcessRequest(BaseModel):
    message: str = "hello"


# ── Routes ────────────────────────────────────────────────────────────────────


@app.post("/process")
def process(client: CogniteClient, logger: logging.Logger, request: ProcessRequest) -> dict:
    """Process incoming data"""
    logger.info("Processing: %s", request.message)
    return {"status": "ok", "message": request.message}


# ── Entry point ───────────────────────────────────────────────────────────────

handle = create_function_service(introspection, app)

__all__ = ["handle"]
