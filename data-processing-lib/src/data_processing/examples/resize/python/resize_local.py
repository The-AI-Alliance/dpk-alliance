import os

from data_processing.data_access import DataAccessLocal
from data_processing.utils import TransformUtils
from resize_transform import ResizeTransform
from data_processing.data_access import compute_data_location


input_folder = compute_data_location("test-data/resize/input")
resize_params = {"max_rows_per_table": 75}
if __name__ == "__main__":
    # Here we show how to run outside of ray
    # Create and configure the transform.
    transform = ResizeTransform(resize_params)

    # Use the local data access to read a parquet table that has 200 rows
    data_access = DataAccessLocal()
    bytes, _ = data_access.get_file(os.path.join(input_folder, "test1.parquet"))
    table = TransformUtils.convert_binary_to_arrow(bytes)

    # Transform the table to tables of 75 rows.  This should give two tables each with 75 rows.
    table_list, _ = transform.transform(table=table)
    print(f"# of output tables: {len(table_list)}")
    # Then flush the buffer which should contain the remaining 50 rows.
    table_list, metadata = transform.flush()
    table = table_list[0]
    print(f"Flushed output table with {table.num_rows} (expecting 50)")
