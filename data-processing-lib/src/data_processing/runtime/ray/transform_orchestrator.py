import time
from logging import Logger
from typing import Any

import ray
from data_processing.data_access import DataAccessFactory
from data_processing.runtime import TransformOrchestrator
from data_processing.runtime.ray import (
    RayTransformExecutionConfiguration,
    RayTransformFileProcessor,
    RayTransformRuntimeConfiguration,
    RayUtils,
    TransformStatisticsRay,
)
from ray.util import ActorPool
from ray.util.metrics import Gauge


class RayTransformOrchestrator(TransformOrchestrator):
    """
    Class implementing transform orchestration for Ray
    """

    def __init__(
        self,
        execution_params: RayTransformExecutionConfiguration,
        data_access_factory: list[DataAccessFactory],
        runtime_config: RayTransformRuntimeConfiguration,
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
        self.statistics = TransformStatisticsRay.remote({})
        # Get Resources for execution
        self.resources = RayUtils.get_cluster_resources()
        self.logger.info(f"Ray cluster resources: {self.resources}")

    def process_data(self) -> None:
        """
        Data processing for Python
        """
        # print execution params
        self.logger.info(
            f"Number of workers - {self.execution_params.n_workers} with {self.execution_params.worker_options} each"
        )
        # create executors
        processor_params = {
            "data_access_factory": self.data_access_factory,
            "transform_class": self.runtime_config.get_transform_class(),
            "transform_params": self.runtime.get_transform_config(
                data_access_factory=self.data_access_factory,
                statistics=self.statistics,
                files=self.files_to_process,
            ),
            "statistics": self.statistics,
            "is_folder": self.is_folder,
        }
        self.logger.debug("Creating actors")
        processors = RayUtils.create_actors(
            clazz=RayTransformFileProcessor,
            params=processor_params,
            actor_options=self.execution_params.worker_options,
            n_actors=self.execution_params.n_workers,
            creation_delay=self.execution_params.creation_delay,
        )
        processors_pool = ActorPool(processors)
        # create gauges
        files_in_progress_gauge = Gauge(
            "files_in_progress", "Number of files in progress"
        )
        files_completed_gauge = Gauge(
            "files_processed_total", "Number of files completed"
        )
        available_cpus_gauge = Gauge("available_cpus", "Number of available CPUs")
        available_gpus_gauge = Gauge("available_gpus", "Number of available GPUs")
        available_memory_gauge = Gauge("available_memory", "Available memory")
        available_object_memory_gauge = Gauge(
            "available_object_store", "Available object store"
        )
        # process data
        self.logger.debug("Begin processing files")
        failures = RayUtils.process_files(
            executors=processors_pool,
            file_producer=self,
            print_interval=self.print_interval,
            files_in_progress_gauge=files_in_progress_gauge,
            files_completed_gauge=files_completed_gauge,
            available_cpus_gauge=available_cpus_gauge,
            available_gpus_gauge=available_gpus_gauge,
            available_memory_gauge=available_memory_gauge,
            object_memory_gauge=available_object_memory_gauge,
            logger=self.logger,
        )
        if failures > 0:
            self._publish_stats({"actor failures": failures})
        self.logger.debug("Done processing files, waiting for flush() completion.")
        # invoke flush to ensure that all results are returned
        start = time.time()
        replies = [processor.flush.remote() for processor in processors]
        failures = RayUtils.wait_for_execution_completion(
            logger=self.logger, replies=replies
        )
        if failures > 0:
            self._publish_stats({"actor failures": failures})
        self.logger.info(f"done flushing in {round(time.time() - start, 3)} sec")

    def _publish_stats(self, stats: dict[str, Any]) -> None:
        """
        Publishing execution statistics
        :param stats: update to Statistics
        :return: None
        """
        if len(stats) > 0:
            self.statistics.add_stats.remote(stats=stats)

    def _get_stats(self) -> dict[str, Any]:
        """
        get statistics
        """
        return ray.get(self.statistics.get_execution_stats.remote())
