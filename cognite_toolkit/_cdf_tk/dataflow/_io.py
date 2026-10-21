import json
from pathlib import Path

from cognite_toolkit._cdf_tk.dataflow._models import SCHEMA_VERSION, Snapshot
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError


def dump_json(snapshot: Snapshot) -> str:
    """Deterministic JSON: equal content gives equal text."""
    return json.dumps(snapshot.model_dump(mode="json", by_alias=True), sort_keys=True, separators=(",", ":"))


def load_json(text: str) -> Snapshot:
    data = json.loads(text)
    version = data.get("schemaVersion") if isinstance(data, dict) else None
    if version != SCHEMA_VERSION:
        raise ToolkitValueError(
            f"Unsupported data flow snapshot schema version {version!r}. This Toolkit reads version {SCHEMA_VERSION}. "
            "Generate the snapshot again with a matching Toolkit version."
        )
    return Snapshot.model_validate(data)


def load_snapshot(path: Path) -> Snapshot:
    return load_json(path.read_text(encoding="utf-8"))


def save_snapshot(snapshot: Snapshot, path: Path) -> bool:
    """Writes the snapshot unless the file already holds the same content. Returns whether it wrote."""
    if path.exists():
        try:
            if load_snapshot(path).content_hash() == snapshot.content_hash():
                return False
        except (ValueError, OSError):
            # An unreadable or old file is replaced.
            pass
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(dump_json(snapshot), encoding="utf-8")
    return True
