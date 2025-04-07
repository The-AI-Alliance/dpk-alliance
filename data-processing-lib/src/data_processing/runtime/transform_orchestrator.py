import time
import traceback
from datetime import datetime
from logging import Logger
from typing import Any

from data_processing.data_access import DataAccessFactory
from data_processing.runtime import TransformExecutionConfiguration
from data_processing.transform import (
    AbstractFolderTransform,
    TransformRuntimeConfiguration,
)
from data_processing.utils import UnrecoverableException, get_logger


logger = get_logger(__name__)


class TransformOrchestrator:
    """
    Class implementing generic functionality of transform orchestration.
    It is a base class for runtime specific orchestration implementations
    """

    def __init__(
        self,
        execution_params: TransformExecutionConfiguration,
        data_access_factory: list[DataAccessFactory],
        runtime_config: TransformRuntimeConfiguration,
        logger: Logger,
    ):
        """
        Init method
        :param execution_params: transform parameters
        :param data_access_factory: data access factory
        :param runtime_config: transformer configuration
        :param logger: logger
        """
        self.logger = logger
        self.execution_params = execution_params
        self.data_access_factory = data_access_factory
        self.start_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.start_time = time.time()
        self.logger.info(f"orchestrator started at {self.start_ts}")
        # create data access
        self.data_access = data_access_factory[0].create_data_access()
        self.data_access_out = data_access_factory[1].create_data_access()
        if self.data_access is None or self.data_access_out is None:
            logger.error("No DataAccess instance provided - exiting")
            raise UnrecoverableException("No DataAccess instance provided - exiting")
        self.data_access.set_output_data_access(self.data_access_out)
        self.runtime_config = runtime_config
        self.runtime = runtime_config.create_transform_runtime()
        self.statistics = None
        self.resources = None
        self.files_to_process = None
        self.print_interval = 0
        self.current_file = -1
        self.is_folder = False

    def get_resources(self) -> None:
        """
        Get resources and statistics (Runtime specific)
        """
        raise ValueError("must be implemented by subclass")

    def get_files_to_process(self) -> int:
        """
        Get files to process
        :return number of files
        """
        self.is_folder = issubclass(
            self.runtime_config.get_transform_class(), AbstractFolderTransform
        )
        if self.is_folder:
            # folder transform
            files = self.runtime.get_folders(data_access=self.data_access)
            self.logger.info(
                f"Number of folders is {len(files)}"
            )  # Get files to process
        else:
            files, profile, retries = self.data_access.get_files_to_process()
            # log retries
            if retries > 0:
                self._publish_stats({"data access retries": retries})
            self.logger.info(
                f"Number of files is {len(files)}, source profile {profile}"
            )
        n_files = len(files)
        if n_files == 0:
            self.logger.error("No input files to process - exiting")
            return 0
        # Print interval
        self.print_interval = int(len(files) / 100)
        if self.print_interval == 0:
            self.print_interval = 1
        self.files_to_process = files
        return n_files

    def next_file(self) -> str:
        """
        Gen next file to process
        :return: file name or None
        """
        self.current_file += 1
        if self.current_file >= len(self.files_to_process):
            return None
        else:
            return self.files_to_process[self.current_file]

    def process_data(self) -> None:
        """
        Data processing (runtime specific)
        """
        raise ValueError("must be implemented by subclass")

    def compute_statistics(self, status: str) -> None:
        """
        Compute execution statistics
        :param status: execution status
        """
        # Compute execution statistics
        self.logger.debug("Computing execution stats")
        stats = self.runtime.compute_execution_stats(self._get_stats())
        if "processing_time" in stats:
            stats["processing_time"] = round(stats["processing_time"], 3)
        # build and save metadata
        self.logger.debug("Building job metadata")
        metadata = {
            "job details": self.execution_params.job_details
            | {
                "start_time": self.start_ts,
                "end_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "status": status,
            },
            "job_input_params": self.runtime_config.get_transform_metadata()
            | self.data_access_factory[0].get_input_params()
            | self.execution_params.get_input_params(),
            "execution_stats": self.resources
            | {"execution time, min": round((time.time() - self.start_time) / 60.0, 3)},
            "job_output_stats": stats,
        }
        self.logger.debug(f"Saving job metadata: {metadata}.")
        self.data_access_out.save_job_metadata(metadata)
        self.logger.debug("Saved job metadata.")

    def _publish_stats(self, stats: dict[str, Any]) -> None:
        """
        Publishing execution statistics
        :param stats: update to Statistics
        :return: None
        """
        raise ValueError("must be implemented by subclass")

    def _get_stats(self) -> dict[str, Any]:
        """
        get statistics
        """
        raise ValueError("must be implemented by subclass")


def orchestrate(
    data_access_factory: list[DataAccessFactory],
    runtime_config: TransformRuntimeConfiguration,
    execution_params: TransformExecutionConfiguration,
    orchestrator: type[TransformOrchestrator],
) -> int:
    """
    Orchestrate
    :param execution_params: transform parameters
    :param data_access_factory: data access factory
    :param runtime_config: transformer configuration
    :param orchestrator: orchestrator implementation
    :return:
    """
    try:
        # create orchestrator executor
        executor = orchestrator(
            execution_params=execution_params,
            data_access_factory=data_access_factory,
            runtime_config=runtime_config,
            logger=logger,
        )
        # get resources
        executor.get_resources()
    except Exception as e:
        logger.error(f"Exception creating orchestrator {e}")
        return 1
    # get files to process
    if executor.get_files_to_process() == 0:
        return 0
    status = "success"
    return_code = 0
    try:
        # process files
        executor.process_data()
    except Exception as e:
        logger.error(f"Exception during data processing {e}: {traceback.print_exc()}")
        return_code = 1
        status = "failure"
    try:
        executor.compute_statistics(status=status)
        return return_code
    except Exception as e:
        logger.error(f"Exception during metadata saving {e}: {traceback.print_exc()}")
        return 1
