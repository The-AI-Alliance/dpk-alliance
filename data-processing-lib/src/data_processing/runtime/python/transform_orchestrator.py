import os
import time
from logging import Logger
from multiprocessing import Pool
from typing import Any

import psutil
from data_processing.data_access import DataAccessFactory
from data_processing.runtime import TransformOrchestrator
from data_processing.runtime.python import (
    PythonPoolTransformFileProcessor,
    PythonTransformExecutionConfiguration,
    PythonTransformFileProcessor,
    PythonTransformRuntimeConfiguration,
)
from data_processing.transform import TransformStatistics
from data_processing.utils import GB


def _execution_resources() -> dict[str, Any]:
    """
    Get Execution resource
    :return: tuple of cpu/memory usage
    """
    # Getting load over15 minutes
    load1, load5, load15 = psutil.getloadavg()
    # Getting memory used
    mused = round(psutil.virtual_memory()[3] / GB, 2)
    return {
        "cpus": round((load15 / os.cpu_count()) * 100, 1),
        "gpus": 0,
        "memory": mused,
        "object_store": 0,
    }


class PythonTransformOrchestrator(TransformOrchestrator):
    """
    Class implementing transform orchestration for Python
    """

    def __init__(
        self,
        execution_params: PythonTransformExecutionConfiguration,
        data_access_factory: list[DataAccessFactory],
        runtime_config: PythonTransformRuntimeConfiguration,
        logger: Logger,
    ):
        super().__init__(
            execution_params=execution_params,
            runtime_config=runtime_config,
            data_access_factory=data_access_factory,
            logger=logger,
        )

    def get_resources(self) -> None:
        """
        Get resources and statistics (Runtime specific)
        """
        # create statistics
        self.statistics = TransformStatistics()
        # get resources
        self.resources = _execution_resources()

    def process_data(self) -> None:
        """
        Data processing for Python
        """

        if self.execution_params.num_processors > 0:
            # using multiprocessor pool for execution
            self._process_data_multiprocessor()
        else:
            # single processor execution
            self._process_data_sequential()

    def _process_data_sequential(self) -> None:
        """
        Process transforms sequentially
        """
        # create executor
        executor = PythonTransformFileProcessor(
            data_access_factory=self.data_access_factory,
            statistics=self.statistics,
            transform_params=self.runtime.get_transform_config(
                data_access_factory=self.data_access_factory,
                statistics=self.statistics,
                files=self.files_to_process,
            ),
            transform_class=self.runtime_config.get_transform_class(),
            is_folder=self.is_folder,
        )
        # process data
        t_start = time.time()
        completed = 0
        path = self.next_file()
        while path is not None:
            executor.process_file(path)
            completed += 1
            if completed % self.print_interval == 0:
                self.logger.info(
                    f"Completed {completed} files in {round((time.time() - t_start) / 60.0, 3)} min"
                )
            path = self.next_file()
        self.logger.info(
            f"Done processing {completed} files, waiting for flush() completion."
        )
        # invoke flush to ensure that all results are returned
        start = time.time()
        executor.flush()
        self.logger.info(f"done flushing in {round(time.time() - start, 3)} sec")

    def _process_data_multiprocessor(self) -> None:
        """
        Process transforms using multiprocessing pool
        """
        # create processor
        processor = PythonPoolTransformFileProcessor(
            data_access_factory=self.data_access_factory,
            transform_params=self.runtime.get_transform_config(
                data_access_factory=self.data_access_factory,
                statistics=self.statistics,
                files=self.files_to_process,
            ),
            transform_class=self.runtime_config.get_transform_class(),
            is_folder=self.is_folder,
        )
        completed = 0
        t_start = time.time()
        # create multiprocessing pool
        size = self.execution_params.num_processors
        with Pool(processes=size) as pool:
            # execute for every input file
            for result in pool.imap_unordered(
                processor.process_file, self.files_to_process
            ):
                completed += 1
                # accumulate statistics
                self._publish_stats(result)
                if completed % self.print_interval == 0:
                    # print intermediate statistics
                    self.logger.info(
                        f"Completed {completed} files ({round(100 * completed / len(self.files_to_process), 2)}%) "
                        f"in {round((time.time() - t_start) / 60.0, 3)} min"
                    )
            self.logger.info(
                f"Done processing {completed} files, waiting for flush() completion."
            )
            results = [{}] * size
            # flush
            for i in range(size):
                results[i] = pool.apply_async(processor.flush)
            for s in results:
                self._publish_stats(s.get())
        self.logger.info(f"done flushing in {time.time() - t_start} sec")

    def _publish_stats(self, stats: dict[str, Any]) -> None:
        """
        Publishing execution statistics
        :param stats: update to Statistics
        :return: None
        """
        if len(stats) > 0:
            self.statistics.add_stats(stats=stats)

    def _get_stats(self) -> dict[str, Any]:
        """
        get statistics
        """
        return self.statistics.get_execution_stats()
