import os

from data_processing.runtime import TransformInvoker
from data_processing.runtime.ray import RayTransformLauncher
from data_processing.utils import get_logger, ParamsUtils
from data_processing.examples.noop.ray import NOOPRayTransformConfiguration
from data_processing.data_access import compute_data_location


logger = get_logger(__name__)


def test_execution():
    input_folder = compute_data_location("test-data/input")
    output_folder = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../output")
    )
    local_conf = {
        "input_folder": input_folder,
        "output_folder": output_folder,
    }
    params = {
        # ®un locally
        "run_locally": True,
        # Data access.
        "data_local_config": ParamsUtils.convert_to_ast(local_conf),
        # noop params
        "noop_sleep_sec": 1,
    }
    invoker = TransformInvoker(launcher=RayTransformLauncher)
    res = invoker.invoke_transform(
        runtime_config=NOOPRayTransformConfiguration(),
        name="noop",
        params=params,
    )
    assert res == 0
