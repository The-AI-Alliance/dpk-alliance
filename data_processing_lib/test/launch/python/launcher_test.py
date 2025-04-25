import os
import sys

from data_processing.runtime.python import PythonTransformLauncher
from data_processing.examples.noop.python import (
    NOOPPythonTransformConfiguration,
)
from data_processing.utils import ParamsUtils


"""
 see: https://stackoverflow.com/questions/55259371/pytest-testing-parser-error-unrecognised-arguments
 to run test using argparse we can simply overwrite sys.argv to supply required arguments
"""
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


def test_launcher():
    params = {
        "data_max_files": -1,
        "data_checkpointing": False,
    }
    # s3 not defined
    sys.argv = ParamsUtils.dict_to_req(d=params)
    res = TestLauncherPython().launch()
    assert 0 == res
    # Add S3 configuration
    params["data_s3_config"] = ParamsUtils.convert_to_ast(s3_conf)
    sys.argv = ParamsUtils.dict_to_req(d=params)
    res = TestLauncherPython().launch()
    assert 1 == res
    # Add S3 credentials
    params["data_s3_cred"] = ParamsUtils.convert_to_ast(s3_cred)
    sys.argv = ParamsUtils.dict_to_req(d=params)
    res = TestLauncherPython().launch()
    assert 0 == res
    # Add local config, should fail because now three different configs exist
    params["data_local_config"] = ParamsUtils.convert_to_ast(local_conf)
    sys.argv = ParamsUtils.dict_to_req(d=params)
    res = TestLauncherPython().launch()
    assert 1 == res
    # remove local config, should still fail, because two configs left
    del params["data_local_config"]
    sys.argv = ParamsUtils.dict_to_req(d=params)
    res = TestLauncherPython().launch()
    assert 0 == res


def test_local_config():
    # test that the driver works with local configuration
    params = {
        "data_local_config": ParamsUtils.convert_to_ast(local_conf),
        "data_max_files": -1,
        "data_checkpointing": False,
    }
    sys.argv = ParamsUtils.dict_to_req(d=params)
    res = TestLauncherPython().launch()
    assert 0 == res


def test_local_config_validate():
    # test the validation of the local configuration
    params = {
        "data_max_files": -1,
        "data_checkpointing": False,
    }
    # invalid local configurations, driver launch should fail with any of these
    local_conf_empty = {}
    local_conf_no_input = {"output_folder": "output_folder"}
    local_conf_no_output = {"input_folder": "input_folder"}
    params["data_local_config"] = ParamsUtils.convert_to_ast(local_conf_empty)
    sys.argv = ParamsUtils.dict_to_req(d=params)
    print(f"parameters {sys.argv}")
    res = TestLauncherPython().launch()
    assert 1 == res
    params["data_local_config"] = ParamsUtils.convert_to_ast(local_conf_no_input)
    sys.argv = ParamsUtils.dict_to_req(d=params)
    res = TestLauncherPython().launch()
    assert 0 == res
    params["data_local_config"] = ParamsUtils.convert_to_ast(local_conf_no_output)
    sys.argv = ParamsUtils.dict_to_req(d=params)
    res = TestLauncherPython().launch()
    assert 0 == res
    params["data_local_config"] = ParamsUtils.convert_to_ast(local_conf)
    sys.argv = ParamsUtils.dict_to_req(d=params)
    res = TestLauncherPython().launch()
    assert 0 == res


def test_s3_config_validate():
    # test the validation of the local configuration
    params = {
        "data_max_files": -1,
        "data_checkpointing": False,
        "data_s3_cred": ParamsUtils.convert_to_ast(s3_cred),
    }
    # invalid local configurations, driver launch should fail with any of these
    s3_conf_empty = {}
    s3_conf_no_input = {"output_folder": "output_folder"}
    s3_conf_no_output = {"input_folder": "input_folder"}
    params["data_s3_config"] = ParamsUtils.convert_to_ast(s3_conf_empty)
    sys.argv = ParamsUtils.dict_to_req(d=params)
    print(f"parameters {sys.argv}")
    res = TestLauncherPython().launch()
    assert 1 == res
    params["data_s3_config"] = ParamsUtils.convert_to_ast(s3_conf_no_input)
    sys.argv = ParamsUtils.dict_to_req(d=params)
    res = TestLauncherPython().launch()
    assert 0 == res
    params["data_s3_config"] = ParamsUtils.convert_to_ast(s3_conf_no_output)
    sys.argv = ParamsUtils.dict_to_req(d=params)
    res = TestLauncherPython().launch()
    assert 0 == res
    params["data_s3_config"] = ParamsUtils.convert_to_ast(s3_conf)
    sys.argv = ParamsUtils.dict_to_req(d=params)
    res = TestLauncherPython().launch()
    assert 0 == res
