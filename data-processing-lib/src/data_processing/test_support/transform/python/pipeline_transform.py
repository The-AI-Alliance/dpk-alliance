from data_processing.runtime.python import (
    PythonTransformLauncher,
    PythonTransformRuntimeConfiguration,
)
from data_processing.test_support.transform.python import (
    NOOPPythonTransformConfiguration,
    ResizePythonTransformConfiguration,
)
from data_processing.transform import PipelineTransformConfiguration
from data_processing.transform.python import PythonPipelineTransform
from data_processing.utils import get_logger


logger = get_logger(__name__)


class ResizeNOOPPythonTransformConfiguration(PythonTransformRuntimeConfiguration):
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
                    ResizePythonTransformConfiguration(),
                    NOOPPythonTransformConfiguration(),
                ],
                transform_class=PythonPipelineTransform,
            )
        )


if __name__ == "__main__":
    launcher = PythonTransformLauncher(ResizeNOOPPythonTransformConfiguration())
    logger.info("Launching resize/noop transform")
    launcher.launch()
