import os

from data_processing.data_access import DataAccessLocal
from data_processing.utils import TransformUtils
from noop_transform import NOOPTransform
from data_processing.data_access import compute_data_location


# create parameters
input_folder = compute_data_location("test-data/noop/input")

noop_params = {"sleep_sec": 1}
if __name__ == "__main__":
    # Here we show how to run outside of the runtime
    # Create and configure the transform.
    transform = NOOPTransform(noop_params)
    # Use the local data access to read a parquet table.
    data_access = DataAccessLocal()
    bytes, _ = data_access.get_file(os.path.join(input_folder, "sample1.parquet"))
    table = TransformUtils.convert_binary_to_arrow(bytes)
    print(f"input table: {table}")
    # Transform the table
    table_list, metadata = transform.transform(table)
    print(f"\noutput table: {table_list}")
    print(f"output metadata : {metadata}")
