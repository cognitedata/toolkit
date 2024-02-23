from pathlib import Path

from cognite.client.data_classes.data_modeling import DataModelId

from cognite_toolkit.cdf_tk.utils import CDFToolConfig


def dump_command(
    ToolGlobals: CDFToolConfig,
    data_model_id: DataModelId,
    output_dir: Path,
    verbose: bool,
) -> None:
    raise NotImplementedError("Not implemented yet")
