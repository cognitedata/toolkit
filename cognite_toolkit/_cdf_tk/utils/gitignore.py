from pathlib import Path

DEFAULT_GITIGNORE_MERGE_HEADER = "# Added by cdf repo init (missing entries)"


def load_gitignore_entries_from_text(content: str) -> list[str]:
    entries: list[str] = []
    seen_entries: set[str] = set()
    for line in content.splitlines():
        normalized = line.strip()
        if not normalized or normalized.startswith("#"):
            continue
        if normalized in seen_entries:
            continue
        entries.append(normalized)
        seen_entries.add(normalized)
    return entries


def append_missing_gitignore_entries(
    entries: list[str], destination: Path, header: str = DEFAULT_GITIGNORE_MERGE_HEADER
) -> bool:
    existing_text = destination.read_text(encoding="utf-8") if destination.exists() else ""
    existing_entries = load_gitignore_entries_from_text(existing_text)

    missing_entries = [entry for entry in entries if entry not in existing_entries]
    if not missing_entries:
        return False

    merged_text = existing_text
    if merged_text and not merged_text.endswith("\n"):
        merged_text += "\n"
    if merged_text:
        merged_text += "\n"
    merged_text += f"{header}\n"
    merged_text += "\n".join(missing_entries)
    merged_text += "\n"
    destination.write_text(merged_text, encoding="utf-8")
    return True
