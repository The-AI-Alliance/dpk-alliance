import os

from data_processing.data_access import DataAccessLocal


def test_table_sampling_data():
    """
    Testing data sampling
    :return: None
    """

    input_folder = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../../test-data/input_multiple")
    )
    output_folder = "/tmp"
    print(input_folder)
    data_access = DataAccessLocal(
        {"input_folder": input_folder, "output_folder": output_folder}
    )
    data_access.set_output_data_access(data_access)
    profile, _ = data_access.sample_input_data()
    print(f"\nprofiled directory {input_folder}")
    print(f"profile {profile}")
    assert profile["estimated number of docs"] == 15.0
    assert profile["max_file_size"] == 0.034458160400390625
    assert profile["min_file_size"] == 0.034458160400390625
    assert profile["total_file_size"] == 0.10337448120117188
    assert profile["average table size MB"] == 0.015768051147460938
