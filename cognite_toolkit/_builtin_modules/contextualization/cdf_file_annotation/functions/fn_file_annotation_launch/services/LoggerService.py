from typing import Literal
import os


class CogniteFunctionLogger:
    def __init__(
        self,
        log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO",
        write: bool = False,
        filepath: str | None = None,
    ):
        self.log_level = log_level.upper()
        self.write = write
        self.filepath = filepath
        self.file_handler = None

        if self.filepath and self.write:
            try:
                dir_name = os.path.dirname(self.filepath)
                if dir_name:
                    os.makedirs(dir_name, exist_ok=True)
                self.file_handler = open(self.filepath, "a", encoding="utf-8")
            except Exception as e:
                print(
                    f"[LOGGER_SETUP_ERROR] Could not open log file {self.filepath}: {e}"
                )
                self.write = False

    def _format_message_lines(self, prefix: str, message: str) -> list[str]:
        formatted_lines = []
        if "\n" not in message:
            formatted_lines.append(f"{prefix} {message}")
        else:
            lines = message.split("\n")
            formatted_lines.append(f"{prefix}{lines[0]}")
            padding = " " * len(prefix)
            for line_content in lines[1:]:
                formatted_lines.append(f"{padding} {line_content}")
        return formatted_lines

    def _print(self, prefix: str, message: str) -> None:
        lines_to_log = self._format_message_lines(prefix, message)
        if self.write and self.file_handler:
            try:
                for line in lines_to_log:
                    print(line)
                    self.file_handler.write(line + "\n")
                self.file_handler.flush()
            except Exception as e:
                print(f"[LOGGER_SETUP_ERROR] Could not write to {self.filepath}: {e}")
        elif not self.write:
            for line in lines_to_log:
                print(line)

    def debug(
        self, message: str, section: Literal["START", "END", "BOTH"] | None = None
    ) -> None:
        if section == "START" or section == "BOTH":
            self._section()
        if self.log_level == "DEBUG":
            self._print("[DEBUG]", message)
        if section == "END" or section == "BOTH":
            self._section()

    def info(
        self, message: str, section: Literal["START", "END", "BOTH"] | None = None
    ) -> None:
        if section == "START" or section == "BOTH":
            self._section()
        if self.log_level in ("DEBUG", "INFO"):
            self._print("[INFO]", message)
        if section == "END" or section == "BOTH":
            self._section()

    def warning(
        self, message: str, section: Literal["START", "END", "BOTH"] | None = None
    ) -> None:
        if section == "START" or section == "BOTH":
            self._section()
        if self.log_level in ("DEBUG", "INFO", "WARNING"):
            self._print("[WARNING]", message)
        if section == "END" or section == "BOTH":
            self._section()

    def error(
        self, message: str, section: Literal["START", "END", "BOTH"] | None = None
    ) -> None:
        if section == "START" or section == "BOTH":
            self._section()
        self._print("[ERROR]", message)
        if section == "END" or section == "BOTH":
            self._section()

    def _section(self) -> None:
        if self.write and self.file_handler:
            self.file_handler.write(
                "--------------------------------------------------------------------------------\n"
            )
        print(
            "--------------------------------------------------------------------------------"
        )

    def close(self) -> None:
        if self.file_handler:
            try:
                self.file_handler.close()
            except Exception as e:
                print(f"[LOGGER_CLEANUP_ERROR] Error closing log file: {e}")
            self.file_handler = None
