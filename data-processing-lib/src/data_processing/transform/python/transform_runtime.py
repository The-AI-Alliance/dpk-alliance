from typing import Any

from data_processing.data_access import DataAccessFactory
from data_processing.transform import TransformRuntime, TransformStatistics


class DefaultPythonTransformRuntime(TransformRuntime):
    """
    Transformer runtime used by processor to create Transform specific environment
    """

    def __init__(self, params: dict[str, Any]):
        """
        Create/config this runtime.
        :param params: parameters, often provided by the CLI arguments as defined by a TransformConfiguration.
        """
        super().__init__(params)

    def get_transform_config(
        self,
        data_access_factory: list[DataAccessFactory],
        statistics: TransformStatistics,
        files: list[str],
    ) -> dict[str, Any]:
        """
        Get the dictionary of configuration that will be provided to the transform's initializer.
        This is the opportunity for this runtime to create a new set of configuration based on the
        config/params provided to this instance's initializer.  This may include the addition
        of new configuration data such as ray shared memory, new actors, etc., that might be needed and
        expected by the transform in its initializer and/or transform() methods.
        :param data_access_factory: data access factory class being used by the RayOrchestrator.
        :param statistics: reference to statistics actor
        :param files - list of files to process
        :return: dictionary of transform init params
        """
        return self.params
