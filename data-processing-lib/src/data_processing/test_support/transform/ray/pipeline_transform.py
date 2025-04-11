from data_processing.transform import PipelineTransformConfiguration
from data_processing.utils import get_logger
from data_processing.runtime.ray import (
    RayTransformLauncher,
    RayTransformRuntimeConfiguration,
)
from data_processing.test_support.transform.ray import (
    NOOPRayTransformConfiguration,
    ResizeRayTransformConfiguration,
)
from data_processing.transform.ray import RayPipelineTransform


logger = get_logger(__name__)


class ResizeNOOPRayTransformConfiguration(RayTransformRuntimeConfiguration):
    """
    Implements the PythonTransformConfiguration for NOOP as required by the PythonTransformLauncher.
    NOOP does not use a RayRuntime class so the superclass only needs the base
    python-only configuration.
    """

    def __init__(self):
        """
        Initialization
        """
        super().__init__(
            transform_config=PipelineTransformConfiguration(
                pipeline=[
                    ResizeRayTransformConfiguration(),
                    NOOPRayTransformConfiguration(),
                ],
                transform_class=RayPipelineTransform,
            )
        )


if __name__ == "__main__":
    # launcher = NOOPRayLauncher()
    launcher = RayTransformLauncher(ResizeNOOPRayTransformConfiguration())
    logger.info("Launching resize/noop transform")
    launcher.launch()
