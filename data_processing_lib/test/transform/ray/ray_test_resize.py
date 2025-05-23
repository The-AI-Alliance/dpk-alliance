from data_processing.test_support.launch.transform_test import (
    AbstractTransformLauncherTest,
)
from data_processing.runtime.ray import RayTransformLauncher
from data_processing.examples.resize.ray import ResizeRayTransformConfiguration
from data_processing.data_access import compute_data_location


class TestRayResizeTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        # The following based on 3 identical input files of about 39kbytes, and 200 rows
        fixtures = []
        common_config = {"runtime_num_workers": 1, "run_locally": True}
        basedir = compute_data_location("test-data/resize")
        launcher = RayTransformLauncher(ResizeRayTransformConfiguration())

        # Split into 4 or so files
        config = {"resize_max_rows_per_table": 125} | common_config
        fixtures.append(
            (launcher, config, basedir + "/input", basedir + "/expected-rows-125")
        )

        # Merge into 2 or so files
        config = {"resize_max_rows_per_table": 300} | common_config
        fixtures.append(
            (launcher, config, basedir + "/input", basedir + "/expected-rows-300")
        )

        # # Merge all into a single table
        config = {"resize_max_mbytes_per_table": 1} | common_config
        fixtures.append(
            (launcher, config, basedir + "/input", basedir + "/expected-mbytes-1")
        )

        # # Merge the 1st 2 and some of the 2nd with the 3rd
        config = {"resize_max_mbytes_per_table": 0.05} | common_config
        fixtures.append(
            (launcher, config, basedir + "/input", basedir + "/expected-mbytes-0.05")
        )

        # Split into 4 or so files
        config = {"resize_max_mbytes_per_table": 0.02} | common_config
        fixtures.append(
            (launcher, config, basedir + "/input", basedir + "/expected-mbytes-0.02")
        )

        return fixtures
