import os
import sys

from data_processing.runtime.python import PythonTransformLauncher
from data_processing.utils import ParamsUtils
from pipeline_transform_fork import PipelineForkPythonTransformConfiguration
from data_processing.data_access import compute_data_location


# create launcher
launcher = PythonTransformLauncher(
    runtime_config=PipelineForkPythonTransformConfiguration()
)
# create parameters
input_folder = compute_data_location("test-data/resize/input")
output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "output"))
local_conf = {
    "input_folder": input_folder,
    "output_folder": output_folder,
}
worker_options = {"num_cpus": 0.8}
params = {
    # Data access. Only required parameters are specified
    "data_local_config": ParamsUtils.convert_to_ast(local_conf),
    # resize configuration
    # "resize_max_mbytes_per_table":  0.02,
    "resize_max_rows_per_table": 250,
    "noop1_sleep_sec": 0,
}
sys.argv = ParamsUtils.dict_to_req(d=params)

# launch
launcher.launch()
