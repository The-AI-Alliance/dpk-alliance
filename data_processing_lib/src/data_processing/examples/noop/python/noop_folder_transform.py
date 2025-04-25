import time
from typing import Any

from data_processing.data_access import DataAccess
from data_processing.runtime.python import (
    PythonTransformLauncher,
    PythonTransformRuntimeConfiguration,
)
from data_processing.examples.noop.python import NOOPTransformConfiguration
from data_processing.transform import AbstractFolderTransform
from data_processing.transform.python import DefaultPythonTransformRuntime
from data_processing.utils import get_logger


logger = get_logger(__name__)


class NOOPFolderTransform(AbstractFolderTransform):
    """
    Implements a simple copy of a pyarrow Table.
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        This is generally called with configuration parsed from the CLI arguments defined
        by the companion runtime, NOOPTransformRuntime.  If running inside the RayMutatingDriver,
        these will be provided by that class with help from the RayMutatingDriver.
        """
        # Make sure that the param name corresponds to the name used in apply_input_params method
        # of NOOPTransformConfiguration class
        super().__init__(config)
        self.sleep = config.get("sleep_sec", 1)
        self.data_access = config.get("data_access")

    def transform(
        self, folder_name: str
    ) -> tuple[list[tuple[bytes, str]], dict[str, Any]]:
        """
        Converts input folder into o or more output files.
        If there is an error, an exception must be raised - exit()ing is not generally allowed.
        :param folder_name: the name of the folder containing arbitrary amount of files.
        :return: a tuple of a list of 0 or more tuples and a dictionary of statistics that will be propagated
                to metadata.  Each element of the return list, is a tuple of the transformed bytes and a string
                holding the file name to use.
        """
        logger.debug(f"Transforming one folder {folder_name}")
        metadata = {}
        # get folder files
        files, retries = self.data_access.get_folder_files(path=folder_name)
        if retries > 0:
            metadata |= {"data access retries": retries}
        result = [()] * len(files)
        index = 0
        for name, file in files.items():
            result[index] = (file, self.data_access.get_output_location(name))
            if self.sleep is not None:
                logger.info(f"Sleep for {self.sleep} seconds")
                time.sleep(self.sleep)
                logger.info("Sleep completed - continue")
            index += 1
        # Add some sample metadata.
        metadata |= {"nfiles": len(files)}
        return result, metadata


class NOOPFolderPythonRuntime(DefaultPythonTransformRuntime):
    def get_folders(self, data_access: DataAccess) -> list[str]:
        """
        Get folders to process
        :param data_access: data access
        :return: list of folders to process
        """
        return [data_access.get_input_folder()]


class NOOPFolderPythonTransformConfiguration(PythonTransformRuntimeConfiguration):
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
            transform_config=NOOPTransformConfiguration(clazz=NOOPFolderTransform),
            runtime_class=NOOPFolderPythonRuntime,
        )


if __name__ == "__main__":
    # launcher = NOOPRayLauncher()
    launcher = PythonTransformLauncher(NOOPFolderPythonTransformConfiguration())
    logger.info("Launching noop transform")
    launcher.launch()
