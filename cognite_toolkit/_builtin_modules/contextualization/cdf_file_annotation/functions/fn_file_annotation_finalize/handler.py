import sys
import threading
from datetime import datetime, timezone, timedelta
from cognite.client import CogniteClient

from dependencies import (
    create_config_service,
    create_logger_service,
    create_write_logger_service,
    create_report_service,
    create_retrieve_service,
    create_apply_service,
    create_general_pipeline_service,
)
from services.FinalizeService import AbstractFinalizeService, GeneralFinalizeService
from services.ApplyService import IApplyService
from services.ReportService import IReportService
from services.RetrieveService import IRetrieveService
from services.PipelineService import IPipelineService
from utils.DataStructures import PerformanceTracker


def handle(data: dict, client: CogniteClient) -> dict:
    """
    Main entry point for the cognite function.
    1. Create an instance of config, logger, and tracker
    2. Create an instance of the finalize function and create implementations of the interfaces
    3. Run the finalize instance until...
        4. It's been 7 minutes
        5. There are no jobs left to process
    6. Generate a report that includes capturing the annotations in RAW
    NOTE: Cognite functions have a run-time limit of 10 minutes.
    Don't want the function to die at the 10minute mark since there's no guarantee all code will execute.
    Thus we set a timelimit of 7 minutes (conservative) so that code execution is guaranteed.
    """
    start_time = datetime.now(timezone.utc)
    log_level = data.get("logLevel", "INFO")

    config_instance, client = create_config_service(function_data=data, client=client)
    logger_instance = create_logger_service(log_level)
    tracker_instance = PerformanceTracker()
    pipeline_instance: IPipelineService = create_general_pipeline_service(
        client, pipeline_ext_id=data["ExtractionPipelineExtId"]
    )

    finalize_instance, report_instance = _create_finalize_service(
        config_instance, client, logger_instance, tracker_instance
    )

    run_status: str = "success"
    try:
        while datetime.now(timezone.utc) - start_time < timedelta(minutes=7):
            if finalize_instance.run():
                return {"status": run_status, "data": data}
            logger_instance.info(tracker_instance.generate_local_report(), "START")
        return {"status": run_status, "data": data}
    except Exception as e:
        run_status = "failure"
        msg = f"Ran into the following error: \n{str(e)}"
        logger_instance.error(message=msg, section="BOTH")
        return {"status": run_status, "message": msg}
    finally:
        result: str = report_instance.update_report()
        logger_instance.info(result)
        overall_report: str = tracker_instance.generate_overall_report()
        logger_instance.info(overall_report, "BOTH")
        # only want to report on the count of successful and failed files in ep_logs since they're relatively short
        parts = overall_report.split("-")
        ep_parts = parts[4:7]
        extracted_string = " - ".join(ep_parts)
        pipeline_instance.update_extraction_pipeline(
            msg=f"(Finalize) {extracted_string}"
        )
        pipeline_instance.upload_extraction_pipeline(status=run_status)


def run_locally(config_file: dict[str, str], log_path: str | None = None):
    """
    Main entry point for local runs/debugging.
    (mimics parallel execution by using threads. Not the same as cognite functions but similar.)
    1. Create an instance of config, logger, and tracker
    2. Create an instance of the finalize function and create implementations of the interfaces
    3. Run the finalize instance until...
        4. There are no jobs left to process
    5. Generate a report that includes capturing the annotations in RAW
    """
    log_level = config_file.get("logLevel", "DEBUG")
    config_instance, client = create_config_service(function_data=config_file)

    if log_path:
        logger_instance = create_write_logger_service(
            log_level=log_level, filepath=log_path
        )
    else:
        logger_instance = create_logger_service(log_level=log_level)

    tracker_instance = PerformanceTracker()

    finalize_instance, report_instance = _create_finalize_service(
        config_instance, client, logger_instance, tracker_instance
    )

    try:
        while True:
            if finalize_instance.run():
                break
            logger_instance.info(tracker_instance.generate_local_report(), "START")
    except Exception as e:
        logger_instance.error(
            message=f"Ran into the following error: \n{e}",
            section="BOTH",
        )
    finally:
        result = report_instance.update_report()
        logger_instance.info(result)
        logger_instance.info(tracker_instance.generate_overall_report(), "BOTH")
        logger_instance.close()


def run_locally_parallel(
    config_file: dict[str, str],
    log_path_1: str | None = None,
    log_path_2: str | None = None,
    log_path_3: str | None = None,
    log_path_4: str | None = None,
):
    thread_1 = threading.Thread(target=run_locally, args=(config_file, log_path_1))
    thread_2 = threading.Thread(target=run_locally, args=(config_file, log_path_2))
    thread_3 = threading.Thread(target=run_locally, args=(config_file, log_path_3))
    thread_4 = threading.Thread(target=run_locally, args=(config_file, log_path_4))

    thread_1.start()
    thread_2.start()
    thread_3.start()
    thread_4.start()

    thread_1.join()
    thread_2.join()
    thread_3.join()
    thread_4.join()


def _create_finalize_service(
    config, client, logger, tracker
) -> tuple[AbstractFinalizeService, IReportService]:
    """
    Instantiate Finalize with interfaces.
    """
    report_instance: IReportService = create_report_service(client, config, logger)
    retrieve_instance: IRetrieveService = create_retrieve_service(
        client, config, logger
    )
    apply_instance: IApplyService = create_apply_service(client, config, logger)
    finalize_instance = GeneralFinalizeService(
        client=client,
        config=config,
        logger=logger,
        tracker=tracker,
        retrieve_service=retrieve_instance,
        apply_service=apply_instance,
        report_service=report_instance,
    )
    return finalize_instance, report_instance


if __name__ == "__main__":
    # NOTE: Receives the arguments from .vscode/launch.json. Mimics arguments that are passed into the serverless function.
    config_file = {
        "ExtractionPipelineExtId": sys.argv[1],
        "logLevel": sys.argv[2],
    }
    run_mode = sys.argv[3]
    log_path_1 = sys.argv[4]
    if run_mode == "Parallel":
        log_path_2 = sys.argv[5]
        log_path_3 = sys.argv[6]
        log_path_4 = sys.argv[7]
        run_locally_parallel(
            config_file, log_path_1, log_path_2, log_path_3, log_path_4
        )
    else:
        run_locally(config_file, log_path_1)
