from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import yaml
from cognite.client import CogniteClient


@dataclass
class ContextConfig:
    extraction_pipeline_ext_id: str
    debug: bool
    run_all: bool
    rawdb: str
    raw_table_good: str
    raw_table_bad: str
    raw_table_manual: str
    time_series_prefix: str
    time_series_data_set_ext_id: str
    asset_root_ext_ids: list[str]
    match_threshold: float

    @classmethod
    def load(cls, data: dict[str, Any]) -> ContextConfig:
        return cls(
            extraction_pipeline_ext_id=data["ExtractionPipelineExtId"],
            debug=data["debug"],
            run_all=data["runAll"],
            rawdb=data["rawdb"],
            raw_table_good=data["rawTableGood"],
            raw_table_bad=data["rawTableBad"],
            raw_table_manual=data["rawTableManual"],
            time_series_prefix=data["timeSeriesPrefix"],
            time_series_data_set_ext_id=data["timeSeriesDataSetExtId"],
            asset_root_ext_ids=data["assetRootExtIds"],
            match_threshold=data["matchThreshold"],
        )


def load_config_parameters(client: CogniteClient, function_data: dict[str, Any]) -> ContextConfig:
    """
    Retrieves the configuration parameters from the function data and loads the configuration from CDF.
    Configuration is loaded from the extraction pipeline configuration and the function data.

    Args:
        client: Instance of CogniteClient
        function_data: dictionary containing the function input configuration data

    Returns:
        ContextConfig object
    """
    try:
        extraction_pipeline_ext_id = function_data["ExtractionPipelineExtId"]
    except KeyError:
        raise ValueError("Missing parameter 'ExtractionPipelineExtId' in function data")

    serialized_config = client.extraction_pipelines.config.retrieve(extraction_pipeline_ext_id)
    if serialized_config:
        data = yaml.safe_load(serialized_config.config)["data"]
        data["ExtractionPipelineExtId"] = extraction_pipeline_ext_id
        return ContextConfig.load(data)
    raise ValueError(f"No configuration found in pipeline {extraction_pipeline_ext_id!r}")
