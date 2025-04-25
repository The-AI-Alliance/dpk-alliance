from data_processing.runtime.python import PythonTransformLauncher
from data_processing.test_support.launch.transform_test import (
    AbstractTransformLauncherTest,
)
from data_processing.examples.noop.python import (
    NOOPFolderPythonTransformConfiguration,
)
from data_processing.data_access import compute_data_location


class TestRayNOOPTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        basedir = compute_data_location("test-data/noop/folders")
        launcher = PythonTransformLauncher(NOOPFolderPythonTransformConfiguration())
        fixtures = [
            (launcher, {"noop_sleep_sec": 0}, basedir + "/input", basedir + "/expected")
        ]
        return fixtures
