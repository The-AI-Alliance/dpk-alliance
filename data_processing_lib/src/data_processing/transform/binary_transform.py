from typing import Any

from data_processing.transform import AbstractTransform


class AbstractBinaryTransform(AbstractTransform):
    """
    Converts input binary file to output file(s) (binary)
    Sub-classes must provide the transform() method to provide the conversion of one binary files to 0 or
    more new binary files.
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        This simply stores the given instance in this instance for later use.
        """
        self.config = config

    def transform_binary(
        self, file_name: str, byte_array: bytes
    ) -> tuple[list[tuple[bytes, str]], dict[str, Any]]:
        """
        Converts input file into o or more output files.
        If there is an error, an exception must be raised - exit()ing is not generally allowed.
        :param byte_array: contents of the input file to be transformed.
        :param file_name: the name of the file containing the given byte_array.
        :return: a tuple of a list of 0 or more tuples and a dictionary of statistics that will be propagated
                to metadata.  Each element of the return list, is a tuple of the transformed bytes and a string
                holding the extension to be used when writing out the new bytes.
        """
        raise NotImplementedError()

    def flush_binary(self) -> tuple[list[tuple[bytes, str]], dict[str, Any]]:
        """
        This is supporting method for transformers, that implement buffering of data, for example coalesce.
        These transformers can have buffers containing data that were not written to the output immediately.
        Flush is the hook for them to return back locally stored data and their statistics.
        The majority of transformers are expected not to use such buffering and can use this default implementation.
        If there is an error, an exception must be raised - exit()ing is not generally allowed.
        :return: a tuple of a list of 0 or more tuples and a dictionary of statistics that will be propagated
                to metadata.  Each element of the return list, is a tuple of the transformed bytes and a string
                holding the extension to be used when writing out the new bytes.
        """
        return [], {}
