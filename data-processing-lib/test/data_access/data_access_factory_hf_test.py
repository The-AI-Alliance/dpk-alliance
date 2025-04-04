import sys
from argparse import ArgumentParser

from data_processing.data_access import DataAccessFactory, DataAccessHF
from data_processing.utils import ParamsUtils


def test_creating_hf_data_access():
    """
    Testing creation of HF data access
    :return: None
    """
    input_folder = "datasets/blublinsky/test/data"
    output_folder = "datasets/blublinsky/test/temp"
    hf_token = "token"
    hf_conf = {
        "hf_token": hf_token,
        "input_folder": input_folder,
        "output_folder": output_folder,
    }
    params = {
        "data_hf_config": hf_conf,
        "data_num_samples": 3,
        "data_files_to_use": [".not_here"],
    }

    # Set the simulated command line args
    sys.argv = ParamsUtils.dict_to_req(params)
    daf = DataAccessFactory()
    parser = ArgumentParser()
    daf.add_input_params(parser)
    args = parser.parse_args()
    daf.apply_input_params(args)

    # create data_access
    data_access = daf.create_data_access()

    # validate created data access
    assert isinstance(data_access, DataAccessHF)
    assert data_access.input_folder == input_folder
    assert data_access.output_folder == output_folder
    assert data_access.fs.token == hf_token

    assert data_access.n_samples == 3
    assert data_access.files_to_use == [".not_here"]
