import hashlib
import json
from pathlib import Path
from typing import Any

from .file import safe_read


def calculate_directory_hash(
    directory: Path,
    exclude_prefixes: set[str] | None = None,
    ignore_files: set[str] | None = None,
    shorten: bool = False,
) -> str:
    sha256_hash = hashlib.sha256()

    # Walk through each file in the directory
    for filepath in sorted(directory.rglob("*"), key=lambda p: str(p.relative_to(directory))):
        if filepath.is_dir():
            continue
        if exclude_prefixes and any(filepath.name.startswith(prefix) for prefix in exclude_prefixes):
            continue
        if ignore_files and filepath.suffix in ignore_files:
            continue
        relative_path = filepath.relative_to(directory)
        sha256_hash.update(relative_path.as_posix().encode("utf-8"))
        # Open each file and update the hash
        with filepath.open("rb") as file:
            while chunk := file.read(8192):
                # Get rid of Windows line endings to make the hash consistent across platforms.
                sha256_hash.update(chunk.replace(b"\r\n", b"\n"))

    calculated = sha256_hash.hexdigest()
    if shorten:
        return calculated[:8]
    return calculated


def calculate_secure_hash(item: dict[str, Any]) -> str:
    """Calculate a secure hash of a dictionary"""
    sha256_hash = hashlib.sha512(usedforsecurity=True)
    sha256_hash.update(json.dumps(item, sort_keys=True).encode("utf-8"))
    return sha256_hash.hexdigest()


def calculate_str_or_file_hash(content: str | Path, shorten: bool = False) -> str:
    sha256_hash = hashlib.sha256()
    if isinstance(content, Path):
        content = safe_read(content)
    # Get rid of Windows line endings to make the hash consistent across platforms.
    sha256_hash.update(content.encode("utf-8").replace(b"\r\n", b"\n"))
    calculated = sha256_hash.hexdigest()
    if shorten:
        return calculated[:8]
    return calculated


def calculate_bytes_or_file_hash(content: bytes | Path, shorten: bool = False) -> str:
    sha256_hash = hashlib.sha256()
    if isinstance(content, Path):
        # Get rid of Windows line endings to make the hash consistent across platforms.
        content = content.read_bytes().replace(b"\r\n", b"\n")
    sha256_hash.update(content)
    calculated = sha256_hash.hexdigest()
    if shorten:
        return calculated[:8]
    return calculated
