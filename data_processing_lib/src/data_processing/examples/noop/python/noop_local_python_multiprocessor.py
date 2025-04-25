import os
import sys

from data_processing.runtime.python import PythonTransformLauncher
from data_processing.utils import ParamsUtils
from noop_transform import NOOPPythonTransformConfiguration
from data_processing.data_access import compute_data_location


# create parameters
input_folder = compute_data_location("test-data/noop/input")
output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "output"))
local_conf = {
    "input_folder": input_folder,
    "output_folder": output_folder,
}
params = {
    # Data access. Only required parameters are specified
    "data_local_config": ParamsUtils.convert_to_ast(local_conf),
    # execution info
    "runtime_num_processors": 2,
    # noop params
    "noop_sleep_sec": 1,
}
if __name__ == "__main__":
    # Set the simulated command line args
    sys.argv = ParamsUtils.dict_to_req(d=params)
    # create launcher
    launcher = PythonTransformLauncher(
        runtime_config=NOOPPythonTransformConfiguration()
    )
    # Launch the ray actor(s) to process the input
    launcher.launch()
