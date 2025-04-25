from data_processing.data_access import DataAccessHF
from data_processing.utils import TransformUtils
from huggingface_hub import CardData


hf_conf = {
    "hf_token": None,
    "input_folder": "datasets/blublinsky/test/data",
    "output_folder": "datasets/blublinsky/test/temp/",
}


def test_hf_data_access():
    """
    Testing data access of HF data access
    :return: None
    """
    data_access = DataAccessHF(hf_config=hf_conf)
    data_access.set_output_data_access(data_access)
    # get files to process
    files, profile, retries = data_access.get_files_to_process()
    assert len(files) == 50
    assert profile["max_file_size"] >= 135.730997085571
    assert profile["min_file_size"] >= 0.00743961334228515
    assert profile["total_file_size"] >= 269.3791465759277

    # read tables
    t_stats = [
        {"n_rows": 8, "n_columns": 11},
        {"n_rows": 424, "n_columns": 11},
        {"n_rows": 9336, "n_columns": 12},
        {"n_rows": 7, "n_columns": 11},
        {"n_rows": 1353, "n_columns": 11},
    ]
    for i in range(5):
        data, retries = data_access.get_file(path=files[i])
        table = TransformUtils.convert_binary_to_arrow(data=data)
        assert table.num_rows == t_stats[i]["n_rows"]
        assert table.num_columns == t_stats[i]["n_columns"]
        if i == 0:
            data, _ = data_access.get_file(path=files[i])
            # write to data set
            output_file = data_access.get_output_location(files[i])
            res, _ = data_access.save_file(path=output_file, data=data)
            assert res is None

    # get random set of files
    random = data_access.get_random_file_set(n_samples=5, files=files)
    assert len(random) == 5


def test_data_set_card():
    """
    Testing data set card access
    :return: None
    """
    # read the card
    data_access = DataAccessHF(hf_config=hf_conf)
    data_access.set_output_data_access(data_access)
    card = data_access.get_dataset_card(ds_name="blublinsky/test")
    assert card.data.license == "apache-2.0"
    # update it
    data = card.data.to_dict()
    data["extension"] = "my_extension"
    card.data = CardData(**data)
    content = card.content
    # save a new card (readme)
    try:
        data_access.update_data_set_card(ds_name="blublinsky/test", content=content)
        # read it back
        card = data_access.get_dataset_card(ds_name="blublinsky/test")
        assert card.data.extension == "my_extension"
    except Exception as e:
        print(f"Exception updating card {e}. Did you specify hf_token?")
