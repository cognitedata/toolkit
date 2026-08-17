from typing import Any

import typer

from ._download_app import DownloadApp
from ._helpers import print_help_if_no_subcommand
from ._purge import PurgeApp
from ._upload_app import UploadApp


class DataApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.main)
        self.add_typer(DownloadApp(*args, **kwargs), name="download")
        self.add_typer(UploadApp(*args, **kwargs), name="upload")
        self.add_typer(PurgeApp(*args, **kwargs), name="purge")

    @staticmethod
    def main(ctx: typer.Context) -> None:
        """Plugin to work with data in CDF"""
        print_help_if_no_subcommand(ctx)
