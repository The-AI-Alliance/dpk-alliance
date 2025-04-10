import os
import sys

from data_processing.runtime.python import PythonTransformLauncher
from data_processing.utils import ParamsUtils
from pipeline_transform import PipelinePythonTransformConfiguration


# create launcher
launcher = PythonTransformLauncher(
    runtime_config=PipelinePythonTransformConfiguration()
)
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
    # Data access. Only required parameters are specified
    "data_local_config": ParamsUtils.convert_to_ast(local_conf),
    # resize configuration
    # "resize_max_mbytes_per_table":  0.02,
    "resize_max_rows_per_table": 300,
}
sys.argv = ParamsUtils.dict_to_req(d=params)

# launch
launcher.launch()
