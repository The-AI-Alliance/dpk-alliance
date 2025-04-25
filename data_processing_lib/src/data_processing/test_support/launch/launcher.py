from data_processing.runtime.python import PythonTransformLauncher
from data_processing.examples.noop.python import (
    NOOPPythonTransformConfiguration,
)
from data_processing.runtime.ray import RayTransformLauncher
from data_processing.examples.noop.ray import NOOPRayTransformConfiguration


class NOOPTestLauncherPython(PythonTransformLauncher):
    """
    Test driver for validation of the functionality
    """

    def __init__(self):
        super().__init__(NOOPPythonTransformConfiguration())

    def _submit_for_execution(self) -> int:
        """
        Overwrite this method to just print all parameters to make sure that everything works
        :return:
        """
        return 0


class NOOPTestLauncherRay(RayTransformLauncher):
    """
    Test driver for validation of the functionality
    """

    def __init__(self):
        super().__init__(NOOPRayTransformConfiguration())

    def _submit_for_execution(self) -> int:
        """
        Overwrite this method to just print all parameters to make sure that everything works
        :return:
        """
        print("\n\nPrinting preprocessing parameters")
        print(f"Run locally {self.run_locally}")
        return 0
