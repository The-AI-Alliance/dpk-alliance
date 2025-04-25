# Resize  files

This is a simple transformer that is resizing the input tables to a specified size. 
* resizing based on in-memory size of the tables.
* resized based on the number of rows in the tables. 

Tables can be either split into smaller sizes or aggregated into larger sizes.


## Configuration and command line Options

The set of dictionary keys holding [ResizeTransform](python/resize_transform.py)
configuration for values are as follows:

* _max_rows_per_table_ - specifies max documents per table
* _max_mbytes_per_table - specifies max size of table, according to the _size_type_ value.
* _size_type_ - indicates how table size is measured. Can be one of
    * memory - table size is measure by the in-process memory used by the table
    * disk - table size is estimated as the on-disk size of the parquet files.  This is an estimate only
        as files are generally compressed on disk and so may not be exact due varying compression ratios.
        This is the default.

Only one of the _max_rows_per_table_ and _max_mbytes_per_table_ may be used.
