import hashlib
import io
import os
import string
import sys
from typing import Any

import mmh3
import pyarrow as pa
import pyarrow.parquet as pq


RANDOM_SEED = 42
LOCAL_TO_DISK = 2

KB = 1024
MB = 1024 * KB
GB = 1024 * MB


class TransformUtils:
    """
    Class implementing support methods for filter implementation
    """

    @staticmethod
    def deep_get_size(ob) -> int:
        """
        Getting the complete size of the Python object. Based on
        https://www.askpython.com/python/built-in-methods/variables-memory-size-in-python
        Supports Python structures: list, tuple and set
            :param ob: object to measure
            :return: object size
        """
        size = sys.getsizeof(ob)
        if isinstance(ob, (list, tuple, set)):
            for element in ob:
                size += TransformUtils.deep_get_size(element)
        if isinstance(ob, dict):
            for k, v in ob.items():
                size += TransformUtils.deep_get_size(k)
                size += TransformUtils.deep_get_size(v)
        return size

    @staticmethod
    def normalize_string(doc: str) -> str:
        """
        string normalization
        :param doc: string to normalize
        :return: normalized string
        """
        return (
            doc.replace(" ", "")
            .replace("\n", "")
            .lower()
            .translate(str.maketrans("", "", string.punctuation))
        )

    @staticmethod
    def str_to_hash(val: str) -> str:
        """
        compute string's hash
        :param val: string
        :return: hash value
        """
        return hashlib.sha256(val.encode("utf-8")).hexdigest()

    @staticmethod
    def str_to_int(s: str) -> int:
        """
        Convert string to int using mmh3 hashing. Ensures predictable result by setting seed
        :param s: string
        :return: int hash
        """
        return mmh3.hash(s, seed=RANDOM_SEED, signed=False)

    @staticmethod
    def decode_content(content_bytes: bytes, encoding: str = "utf-8") -> str:
        """
        Decode the given bytes content using the specified encoding.
        :param content_bytes: The bytes content to decode
        :param encoding:The encoding to use while decoding the content. Default is 'utf-8'
        :return: str: The decoded content as a string if successful,
                      otherwise empty string if an error occurs during decoding.
        """
        try:
            content_string = content_bytes.decode(encoding)
            return content_string
        except Exception:
            return ""

    @staticmethod
    def get_file_extension(file_path) -> tuple[str, str]:
        """
        Get the file's root and extension from the given file path.
        :param file_path : The path of the file.
        :return: str: The file extension including the dot ('.') if present, otherwise an empty string.
        """
        return os.path.splitext(file_path)

    @staticmethod
    def get_file_basename(file_path) -> str:
        """
        Get the file's base name from the given file path.
        :param file_path : The path of the file.
        :return: str: file base name.
        """
        return os.path.basename(file_path)

    @staticmethod
    def validate_columns(table: pa.Table, required: list[str]) -> None:
        """
        Check if required columns exist
        :param table: table
        :param required: list of required columns
        :return: None
        """
        columns = table.schema.names
        result = True
        for r in required:
            if r not in columns:
                result = False
                break
        if not result:
            raise Exception(
                f"Not all required columns are present in the table - "
                f"required {required}, present {columns}"
            )

    @staticmethod
    def convert_binary_to_arrow(data: bytes, schema: pa.schema = None) -> pa.Table:
        """
        Convert byte array to table
        :param data: byte array
        :param schema: optional Arrow table schema used for reading table, default None
        :return: table or None if the conversion failed
        """
        from data_processing.utils import get_logger

        logger = get_logger(__name__)
        try:
            reader = pa.BufferReader(data)
            table = pq.read_table(reader, schema=schema)
            return table
        except Exception as e:
            logger.warning(f"Could not convert bytes to pyarrow: {e}")

        # We have seen this exception before when using pyarrow, but polars does not throw it.
        # "Nested data conversions not implemented for chunked array outputs"
        # See issue 816 https://github.com/IBM/data-prep-kit/issues/816.
        logger.info("Attempting read of pyarrow Table using polars")
        try:
            import polars

            df = polars.read_parquet(io.BytesIO(data))
            table = df.to_arrow()
        except Exception as e:
            logger.error(
                f"Could not convert bytes to pyarrow using polars: {e}. Skipping."
            )
            table = None
        return table

    @staticmethod
    def convert_arrow_to_binary(table: pa.Table) -> bytes:
        """
        Convert Arrow table to byte array
        :param table: Arrow table
        :return: byte array or None if conversion fails
        """
        from data_processing.utils import get_logger

        logger = get_logger(__name__)
        try:
            # convert table to bytes
            writer = pa.BufferOutputStream()
            # Update default snappy compression to ZSTD.
            # See https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_table.html
            pq.write_table(table=table, where=writer, compression="ZSTD")
            return bytes(writer.getvalue())
        except Exception as e:
            logger.error(
                f"Failed to convert arrow table to byte array, exception {e}. Skipping it"
            )
            return None

    @staticmethod
    def add_column(table: pa.Table, name: str, content: list[Any]) -> pa.Table:
        """
        Add column to the table
        :param table: original table
        :param name: column name
        :param content: content of the column
        :return: updated table, containing new column
        """
        # check if column already exist and drop it
        if name in table.schema.names:
            table = table.drop(columns=[name])
        # append column
        return table.append_column(field_=name, column=[content])

    @staticmethod
    def verify_no_duplicate_columns(table: pa.Table, file: str) -> bool:
        """
        Verify that resulting table does not have duplicate columns
        :param table: table
        :param file: file where we are saving the table
        :return: True, if there are no duplicates, False otherwise
        """
        from data_processing.utils import get_logger

        logger = get_logger(__name__)
        columns_list = table.schema.names
        columns_set = set(columns_list)
        if len(columns_set) != len(columns_list):
            logger.warning(
                f"Resulting table for file {file} contains duplicate columns {columns_list}. Skipping"
            )
            return False
        return True

    @staticmethod
    def clean_path(path: str) -> str:
        """
        Clean path parameters:
            Removes white spaces from the input/output paths
            Removes schema prefix (s3://, http:// https://), if exists
            Adds the "/" character at the end, if it doesn't exist
            Removes URL encoding
        :param path: path to clean up
        :return: clean path
        """
        path = path.strip()
        if path == "":
            return path
        from urllib.parse import unquote, urlparse, urlunparse

        # Parse the URL
        parsed_url = urlparse(path)
        if parsed_url.scheme in ["http", "https"]:
            # Remove host
            parsed_url = parsed_url._replace(netloc="")
            parsed_url = parsed_url._replace(path=parsed_url.path[1:])

        # Remove the schema
        parsed_url = parsed_url._replace(scheme="")

        # Reconstruct the URL without the schema
        url_without_schema = urlunparse(parsed_url)

        # Remove //
        if url_without_schema[:2] == "//":
            url_without_schema = url_without_schema.replace("//", "", 1)

        return_path = unquote(url_without_schema)
        return TransformUtils.ensure_slash(return_path)

    @staticmethod
    def ensure_slash(path: str) -> str:
        if path[-1] != "/":
            path += "/"
        return path
