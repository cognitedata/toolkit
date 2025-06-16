from pathlib import Path

from cognite_toolkit._cdf_tk.utils.hashing import calculate_hash, calculate_str_or_file_hash


def test_hash_filepath_equals_windows_line_endings(tmp_path: Path) -> None:
    # This test ensures that the hash of a file with Windows line endings is the same as the hash of the same file
    # with Unix line endings.
    my_file = tmp_path / "test_file.txt"
    my_file.write_text("Hello, World!\r\n", encoding="utf-8", newline="\r\n")

    bytes_method = calculate_hash(my_file, shorten=True)
    str_method = calculate_str_or_file_hash(my_file, shorten=True)
    assert bytes_method == str_method, "Hash mismatch between bytes and str methods for file with Windows line endings"
