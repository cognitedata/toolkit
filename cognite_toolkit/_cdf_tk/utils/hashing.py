import hashlib
import json
import zipfile
from pathlib import Path
from typing import Any


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


def calculate_secure_hash(item: dict[str, Any], shorten: bool = False) -> str:
    """Calculate a secure hash of a dictionary"""
    sha256_hash = hashlib.sha512(usedforsecurity=True)
    sha256_hash.update(json.dumps(item, sort_keys=True).encode("utf-8"))
    calculated_hash = sha256_hash.hexdigest()
    if shorten:
        return calculated_hash[:8]
    return calculated_hash


def calculate_hash(content: str | bytes | Path, shorten: bool = False) -> str:
    sha256_hash = hashlib.sha256()
    if isinstance(content, Path):
        if content.suffix == ".zip":
            return calculate_zipfile_hash(content, shorten=shorten)
        # Get rid of Windows line endings to make the hash consistent across platforms.
        content = content.read_bytes().replace(b"\r\n", b"\n")
    elif isinstance(content, str):
        content = content.encode("utf-8")
    sha256_hash.update(content)
    calculated = sha256_hash.hexdigest()
    if shorten:
        return calculated[:8]
    return calculated


def calculate_zipfile_hash(filepath: Path, shorten: bool = False) -> str:
    """Calculate a hash of a zip file based on its contents, ignoring zip metadata.

    It reads the contents directly from the zip file without extracting,
    which ensures consistent hashing across platforms.
    """
    sha256_hash = hashlib.sha256()

    with zipfile.ZipFile(filepath) as zip_file:
        # Sort by name to ensure consistent ordering across platforms
        for info in sorted(zip_file.infolist(), key=lambda x: x.filename):
            # Skip directories
            if info.is_dir():
                continue
            # Include the filename in the hash (using forward slashes for consistency)
            sha256_hash.update(info.filename.replace("\\", "/").encode("utf-8"))
            # Read and hash the file contents
            content = zip_file.read(info.filename)
            # Normalize line endings for consistent hashing across platforms
            sha256_hash.update(content.replace(b"\r\n", b"\n"))

    calculated_hash = sha256_hash.hexdigest()
    if shorten:
        return calculated_hash[:8]
    return calculated_hash
