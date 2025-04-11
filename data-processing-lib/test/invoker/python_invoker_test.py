import os

from data_processing.runtime import TransformInvoker
from data_processing.runtime.python import PythonTransformLauncher
from data_processing.utils import get_logger, ParamsUtils
from data_processing.examples.noop.python import (
    NOOPPythonTransformConfiguration,
)


logger = get_logger(__name__)


def test_execution():
    input_folder = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../../test-data/input")
    )
    output_folder = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../output")
    )
    local_conf = {
        "input_folder": input_folder,
        "output_folder": output_folder,
    }
    params = {
        # Data access.
        "data_local_config": ParamsUtils.convert_to_ast(local_conf),
        # noop params
        "noop_sleep_sec": 1,
    }
    invoker = TransformInvoker(launcher=PythonTransformLauncher)
    res = invoker.invoke_transform(
        runtime_config=NOOPPythonTransformConfiguration(),
        name="noop",
        params=params,
    )
    assert res == 0
