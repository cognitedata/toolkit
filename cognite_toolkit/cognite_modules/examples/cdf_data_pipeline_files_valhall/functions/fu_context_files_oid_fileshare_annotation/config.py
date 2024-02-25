from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import yaml
from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError


@dataclass
class AnnotationConfig:
    extpipe_xid: str
    debug: bool
    run_all: bool
    doc_limit: int
    doc_data_set_ext_id: str
    doc_type_meta_col: str
    p_and_id_doc_type: str
    asset_root_ext_ids: list[str]
    match_threshold: float

    @classmethod
    def load(cls, data: dict[str, Any]) -> AnnotationConfig:
        return cls(
            extpipe_xid=data["ExtractionPipelineExtId"],
            debug=data["debug"],
            run_all=data["runAll"],
            doc_limit=data["docLimit"],
            doc_data_set_ext_id=data["docDataSetExtId"],
            doc_type_meta_col=data["docTypeMetaCol"],
            p_and_id_doc_type=data["pAndIdDocType"],
            asset_root_ext_ids=data["assetRootExtIds"],
            match_threshold=data["matchThreshold"],
        )


def load_config_parameters(cognite_client: CogniteClient, function_data: dict[str, Any]) -> AnnotationConfig:
    """Retrieves the configuration parameters from the function data and loads the configuration from CDF."""
    if "ExtractionPipelineExtId" not in function_data:
        raise ValueError("Missing key 'ExtractionPipelineExtId' in input data to the function")

    extpipe_xid = function_data["ExtractionPipelineExtId"]
    try:
        extpipe_config = cognite_client.extraction_pipelines.config.retrieve(extpipe_xid)
    except CogniteAPIError:
        raise RuntimeError(f"Not able to retrieve pipeline config for extraction pipeline: {extpipe_xid!r}")

    data = yaml.safe_load(extpipe_config.config)["data"]
    data["ExtractionPipelineExtId"] = extpipe_xid
    return AnnotationConfig.load(data)
