from data_processing.data_access import DataAccess
from data_processing.examples.noop.python import (
    NOOPFolderTransform,
    NOOPTransformConfiguration,
)
from data_processing.utils import get_logger
from data_processing.runtime.ray import (
    RayTransformLauncher,
    RayTransformRuntimeConfiguration,
)
from data_processing.transform.ray import DefaultRayTransformRuntime


logger = get_logger(__name__)


class NOOPFolderRayRuntime(DefaultRayTransformRuntime):
    def get_folders(self, data_access: DataAccess) -> list[str]:
        """
        Get folders to process
        :param data_access: data access
        :return: list of folders to process
        """
        return [data_access.get_input_folder()]


class NOOPFolderRayTransformConfiguration(RayTransformRuntimeConfiguration):
    """
    Implements the RayTransformConfiguration for NOOP as required by the RayTransformLauncher.
    NOOP does not use a RayRuntime class so the superclass only needs the base
    python-only configuration.
    """

    def __init__(self):
        """
        Initialization
        """
        super().__init__(
            transform_config=NOOPTransformConfiguration(clazz=NOOPFolderTransform),
            runtime_class=NOOPFolderRayRuntime,
        )


if __name__ == "__main__":
    # launcher = NOOPRayLauncher()
    launcher = RayTransformLauncher(NOOPFolderRayTransformConfiguration())
    logger.info("Launching noop transform")
    launcher.launch()
