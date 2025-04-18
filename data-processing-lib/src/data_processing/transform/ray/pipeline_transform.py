from typing import Any

import ray
from data_processing.transform import AbstractPipelineTransform, TransformRuntime


class RayPipelineTransform(AbstractPipelineTransform):
    """
    Transform that executes a set of base transforms sequentially. Data is passed between
    participating transforms in memory
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initializes pipeline execution for the list of transforms
        :param config - configuration parameters - list of transforms in the pipeline.
        Note that transforms will be executed in the order they are defined
        """
        super().__init__(config)

    def _get_transform_params(self, runtime: TransformRuntime) -> dict[str, Any]:
        """
        get transform parameters
        :param runtime - runtime
        :return: transform params
        """
        return runtime.get_transform_config(
            data_access_factory=self.data_access_factory,
            statistics=self.statistics,
            files=[],
        )

    def _compute_execution_statistics(self, stats: dict[str, Any]) -> None:
        """
        Compute execution statistics
        :param stats: current statistics from flush
        :return: None
        """
        current = ray.get(self.statistics.get_execution_stats.remote())
        current |= stats
        for transform in self.participants:
            for t in transform:
                current = t[1].compute_execution_stats(stats=current)
        ray.get(self.statistics.update_stats.remote(current))
