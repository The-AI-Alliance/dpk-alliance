import os
import sys

from data_processing.utils import ParamsUtils
from data_processing.runtime.ray import RayTransformLauncher
from pipeline_transform import PipelineRayTransformConfiguration


# create launcher
launcher = RayTransformLauncher(runtime_config=PipelineRayTransformConfiguration())
# create parameters
input_folder = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../../../test-data/resize/input")
)
output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "output"))
local_conf = {
    "input_folder": input_folder,
    "output_folder": output_folder,
}
worker_options = {"num_cpus": 0.8}
code_location = {"github": "github", "commit_hash": "12345", "path": "path"}
params = {
    # where to run
    "run_locally": True,
    # Data access. Only required parameters are specified
    "data_local_config": ParamsUtils.convert_to_ast(local_conf),
    # orchestrator
    "runtime_worker_options": ParamsUtils.convert_to_ast(worker_options),
    "runtime_num_workers": 2,
    "runtime_creation_delay": 0,
    # resize configuration
    # "resize_max_mbytes_per_table":  0.02,
    "resize_max_rows_per_table": 125,
}
sys.argv = ParamsUtils.dict_to_req(d=params)

# launch
launcher.launch()
