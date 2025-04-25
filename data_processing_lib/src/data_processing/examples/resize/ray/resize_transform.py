from data_processing.utils import get_logger
from data_processing.runtime.ray import (
    RayTransformLauncher,
    RayTransformRuntimeConfiguration,
)
from data_processing.examples.resize.python import ResizeTransformConfiguration


logger = get_logger(__name__)


class ResizeRayTransformConfiguration(RayTransformRuntimeConfiguration):
    """
    Implements the RayTransformConfiguration for resize as required by the RayTransformLauncher.
    Resize does not use a RayRuntime class so the superclass only needs the base
    python-only configuration.
    """

    def __init__(self):
        """
        Initialization
        """
        super().__init__(transform_config=ResizeTransformConfiguration())


if __name__ == "__main__":
    launcher = RayTransformLauncher(ResizeRayTransformConfiguration())
    launcher.launch()
