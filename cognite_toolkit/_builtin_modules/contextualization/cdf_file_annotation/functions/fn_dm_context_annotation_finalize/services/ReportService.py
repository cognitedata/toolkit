import abc

from cognite.client import CogniteClient
from cognite.client.data_classes import RowWrite

from services.ConfigService import Config
from services.LoggerService import CogniteFunctionLogger


class IReportService(abc.ABC):
    """
    Interface for reporting the annotations that have been applied
    e.g.) Used as the numerator for annotation link rate at Marathon
    """

    @abc.abstractmethod
    def add_annotations(
        self, doc_rows: list[RowWrite], tag_rows: list[RowWrite]
    ) -> None:
        pass

    @abc.abstractmethod
    def delete_annotations(
        self,
        doc_row_keys: list[str],
        tag_row_keys: list[str],
    ) -> None:
        pass

    @abc.abstractmethod
    def update_report(self) -> str:
        pass


class GeneralReportService(IReportService):
    """
    Interface for reporting the annotations that have been applied
    e.g.) Used as the numerator for annotation link rate at Marathon
    """
    
    def __init__(
        self, client: CogniteClient, config: Config, logger: CogniteFunctionLogger
    ):
        self.client = client
        self.config = config
        self.logger = logger

        self.db: str = config.finalize_function.report_service.raw_db
        self.doc_table: tuple[str, list[RowWrite], list[str]] = (
            config.finalize_function.report_service.raw_table_doc_doc,
            [],
            [],
        )
        self.tag_table: tuple[str, list[RowWrite], list[str]] = (
            config.finalize_function.report_service.raw_table_doc_tag,
            [],
            [],
        )
        self.batch_size: int = config.finalize_function.report_service.raw_batch_size
        self.delete: bool = self.config.finalize_function.clean_old_annotations

    def add_annotations(
        self, doc_rows: list[RowWrite], tag_rows: list[RowWrite]
    ) -> None:
        """
        NOTE:   Using batch size to ensure that we're writing to raw efficiently. IMO report doesn't need to be pushed to raw at the end of every diagram detect job.
                Though we don't want to be too efficient to where we lose out on data in case anything happens to the thread. Thus this balances efficiency with data secureness.
                Updating report at the end of every job with 50 files that's processed leads to around 15 seconds of additional time added.
                Thus, for 61,000 files / 50 files per job = 1220 jobs * 15 seconds added = 18300 seconds = 305 minutes saved by writing to RAW more efficiently.
        """
        self.doc_table[1].extend(doc_rows)
        self.tag_table[1].extend(tag_rows)
        if len(self.doc_table[1]) + len(self.tag_table[1]) > self.batch_size:
            msg = self.update_report()
            self.logger.info(f"{msg}", "BOTH")
        return

    def delete_annotations(
        self,
        doc_row_keys: list[str],
        tag_row_keys: list[str],
    ) -> None:
        self.doc_table[2].extend(doc_row_keys)
        self.tag_table[2].extend(tag_row_keys)
        return

    def update_report(self) -> str:
        """
        Upload annotation edges to RAW for reporting.
        If clean old annotations is set to true, delete the rows before uploading the rows in RAW.
        NOTE: tuple meaning -> self.doc_table[0] = tbl_name, [1] = rows to upload, [2] = keys of the rows to delete
        """
        delete_msg = None
        if self.delete:
            self.client.raw.rows.delete(
                db_name=self.db,
                table_name=self.doc_table[0],
                key=self.doc_table[2],
            )
            self.client.raw.rows.delete(
                db_name=self.db,
                table_name=self.tag_table[0],
                key=self.tag_table[2],
            )
            delete_msg = f"Deleted annotations from db: {self.db}\n- deleted {len(self.doc_table[2])} rows from tbl: {self.doc_table[0]}\n- deleted {len(self.tag_table[2])} rows from tbl: {self.tag_table[0]}"

        update_msg = "No annotations to upload"
        if len(self.doc_table[1]) > 0 or len(self.tag_table[1]) > 0:
            update_msg = f"Uploaded annotations to db: {self.db}\n- added {len(self.doc_table[1])} rows to tbl: {self.doc_table[0]}\n- added {len(self.tag_table[1])} rows to tbl: {self.tag_table[0]}"
            self.client.raw.rows.insert(
                db_name=self.db,
                table_name=self.doc_table[0],
                row=self.doc_table[1],
                ensure_parent=True,
            )
            self.client.raw.rows.insert(
                db_name=self.db,
                table_name=self.tag_table[0],
                row=self.tag_table[1],
                ensure_parent=True,
            )
        self._clear_tables()

        if delete_msg:
            return f" {delete_msg}\n{update_msg}"
        return f" {update_msg}"

    def _clear_tables(self) -> None:
        self.doc_table[1].clear()
        self.tag_table[1].clear()
        if self.delete:
            self.doc_table[2].clear()
            self.tag_table[2].clear()
