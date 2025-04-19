from data_processing.data_access import DataAccessS3
from moto import mock_aws
from data_processing.data_access import compute_data_location


s3_cred = {
    "access_key": "access",
    "secret_key": "secret",
    "region": "us-east-1",
}

s3_conf = {
    "input_folder": "test/table_read_write/input/",
    "output_folder": "test/table_read_write/output/",
}


def _create_and_populate_files(
    d_a: DataAccessS3, input_location: str, n_files: int
) -> None:
    # create bucket
    d_a.arrS3.s3_client.create_bucket(Bucket="test")
    # upload file
    loc = compute_data_location("test-data/input/sample1.parquet")
    with open(loc, "rb") as file:
        bdata = file.read()
    for i in range(n_files):
        d_a.save_file(path=f"{input_location}sample{i}.parquet", data=bdata)


def test_file_read_write():
    """
    Testing table read/write
    :return: None
    """
    with mock_aws():
        # create data access
        d_a = DataAccessS3(
            s3_credentials=s3_cred, s3_config=s3_conf, checkpoint=False, m_files=-1
        )
        d_a.set_output_data_access(d_a)
        # populate bucket
        input_location = "test/table_read_write/input/"
        _create_and_populate_files(d_a=d_a, input_location=input_location, n_files=1)
        # read the table
        input_location = f"{input_location}sample0.parquet"
        data, retries = d_a.get_file(path=input_location)
        print(f"\nGot file with the length {len(data)}")
        assert 0 == retries
        assert 36132 == len(data)
        # get table output location
        output_location = d_a.get_output_location(input_location)
        print(f"Output location {output_location}")
        assert "test/table_read_write/output/sample0.parquet" == output_location
        # save the table
        result, retries = d_a.save_file(path=output_location, data=data)
        print(f"saving file result {result}")
        assert 0 == retries
        assert result is not None


def test_get_folder():
    """
    Testing get folder
    :return: None
    """
    with mock_aws():
        # create data access
        d_a = DataAccessS3(s3_credentials=s3_cred, checkpoint=False, m_files=-1)
        d_a.set_output_data_access(d_a)
        # populate bucket
        input_location = "test/table_read_write/input/"
        _create_and_populate_files(d_a=d_a, input_location=input_location, n_files=3)
        # get the folder
        files, _ = d_a.get_folder_files(path=input_location, extensions=[".parquet"])
        print(f"\ngot {len(files)} files")
        assert 3 == len(files)


def test_files_to_process():
    """
    Testing get files to process
    :return: None
    """
    with mock_aws():
        # create data access
        d_a = DataAccessS3(
            s3_credentials=s3_cred, s3_config=s3_conf, checkpoint=False, m_files=-1
        )
        d_a.set_output_data_access(d_a)
        # populate bucket
        _create_and_populate_files(
            d_a=d_a, input_location=f"{s3_conf['input_folder']}dataset=d1/", n_files=4
        )
        _create_and_populate_files(
            d_a=d_a, input_location=f"{s3_conf['input_folder']}dataset=d2/", n_files=4
        )
        # get files to process
        files, profile, _ = d_a.get_files_to_process()
        print(f"\nfiles {len(files)}, profile {profile}")
        assert 8 == len(files)
        assert 0.034458160400390625 == profile["max_file_size"]
        assert 0.034458160400390625 == profile["min_file_size"]
        assert 0.275665283203125 == profile["total_file_size"]
        # use checkpoint
        # populate bucket
        _create_and_populate_files(
            d_a=d_a, input_location=f"{s3_conf['output_folder']}dataset=d2/", n_files=2
        )
        d_a.checkpoint = True
        files, profile, _ = d_a.get_files_to_process()
        print(f"files with checkpointing {len(files)}, profile {profile}")
        assert 6 == len(files)
        assert 0.034458160400390625 == profile["max_file_size"]
        assert 0.034458160400390625 == profile["min_file_size"]
        assert 0.20674896240234375 == profile["total_file_size"]
