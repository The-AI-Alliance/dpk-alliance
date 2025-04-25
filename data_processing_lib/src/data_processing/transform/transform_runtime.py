from typing import Any

from data_processing.data_access import DataAccess, DataAccessFactory


class TransformRuntime:
    """
    Transformer runtime used by processor to create Transform specific environment
    """

    def __init__(self, params: dict[str, Any]):
        """
        Create/config this runtime.
        :param params: parameters, often provided by the CLI arguments as defined by a TableTransformConfiguration.
        """
        self.params = params

    def get_folders(self, data_access: DataAccess) -> list[str]:
        """
        Get folders to process
        :param data_access: data access
        :return: list of folders to process
        """
        raise NotImplementedError()

    def get_transform_config(
        self, statistics, data_access_factory: list[DataAccessFactory], files: list[str]
    ) -> dict[str, Any]:
        """
        Get the dictionary of configuration that will be provided to the transform's initializer.
        This is the opportunity for this runtime to create a new set of configuration based on the
        config/params provided to this instance's initializer.  This may include the addition
        of new configuration data such as ray shared memory, new actors, etc., that might be needed and
        expected by the transform in its initializer and/or transform() methods
        :param data_access_factory - data access factory class being used by the RayOrchestrator
        :param statistics - reference to statistics actor
        :param files - list of files to process
        :return: dictionary of transform init params
        """
        return self.params

    def compute_execution_stats(self, stats: dict[str, Any]) -> dict[str, Any]:
        """
        Update/augment the given stats object with runtime-specific additions/modifications.
        :param stats: output of statistics as aggregated across all calls to all transforms.
        :return: job execution statistics.  These are generally reported as metadata by the Ray Orchestrator.
        """
        return stats
