from data_processing.utils import get_logger
from data_processing.runtime.ray import (
    RayTransformLauncher,
    RayTransformRuntimeConfiguration,
)
from data_processing.examples.noop.python import NOOPTransformConfiguration


logger = get_logger(__name__)


class NOOPRayTransformConfiguration(RayTransformRuntimeConfiguration):
    """
    Implements the RayTransformConfiguration for NOOP as required by the RayTransformLauncher.
    NOOP does not use a RayRuntime class so the superclass only needs the base
    python-only configuration.
    """

    def __init__(self):
        """
        Initialization
        """
        super().__init__(transform_config=NOOPTransformConfiguration())


if __name__ == "__main__":
    # launcher = NOOPRayLauncher()
    launcher = RayTransformLauncher(NOOPRayTransformConfiguration())
    logger.info("Launching noop transform")
    launcher.launch()
