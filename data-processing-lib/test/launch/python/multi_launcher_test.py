import os

from data_processing.runtime import multi_launcher
from data_processing.runtime.python import PythonTransformLauncher
from data_processing.test_support.transform.python import (
    NOOPPythonTransformConfiguration,
)
from data_processing.utils import ParamsUtils


s3_cred = {
    "access_key": "access",
    "secret_key": "secret",
    "url": "https://s3.us-east.cloud-object-storage.appdomain.cloud",
}
s3_conf = {
    "input_folder": "input_folder",
    "output_folder": "output_folder",
}
local_conf = {
    "input_folder": os.path.join(os.sep, "tmp", "input"),
    "output_folder": os.path.join(os.sep, "tmp", "output"),
}

code_location = {"github": "github", "commit_hash": "12345", "path": "path"}


class TestLauncherPython(PythonTransformLauncher):
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


def test_multi_launcher():
    params = {
        "data_max_files": -1,
        "data_checkpointing": False,
        "data_s3_cred": ParamsUtils.convert_to_ast(s3_cred),
        "data_s3_config": [ParamsUtils.convert_to_ast(s3_conf)],
    }
    # s3 configuration
    res = multi_launcher(params=params, launcher=TestLauncherPython())
    assert 1 == res
    params = {
        "data_max_files": -1,
        "data_checkpointing": False,
        "data_local_config": [ParamsUtils.convert_to_ast(local_conf)],
    }
    # local configuration
    res = multi_launcher(params=params, launcher=TestLauncherPython())
    assert 1 == res
