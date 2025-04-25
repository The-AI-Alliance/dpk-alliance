import os
import sys

from data_processing.utils import ParamsUtils
from data_processing.runtime.ray import RayTransformLauncher
from noop_transform import NOOPRayTransformConfiguration
from data_processing.data_access import compute_data_location


# create parameters
input_folder = compute_data_location("test-data/noop/input")
output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "output"))
local_conf = {
    "input_folder": input_folder,
    "output_folder": output_folder,
}
worker_options = {"num_cpus": 0.8}
params = {
    # where to run
    "run_locally": True,
    # Data access. Only required parameters are specified
    "data_local_config": ParamsUtils.convert_to_ast(local_conf),
    # orchestrator
    "runtime_worker_options": ParamsUtils.convert_to_ast(worker_options),
    "runtime_num_workers": 3,
    # noop params
    "noop_sleep_sec": 1,
}
if __name__ == "__main__":
    # Set the simulated command line args
    sys.argv = ParamsUtils.dict_to_req(d=params)
    # create launcher
    launcher = RayTransformLauncher(NOOPRayTransformConfiguration())
    # Launch the ray actor(s) to process the input
    launcher.launch()
