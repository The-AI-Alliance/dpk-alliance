from typing import Any

from data_processing.transform import AbstractTransform


class AbstractFolderTransform(AbstractTransform):
    """
    Converts input folder to output file(s) (binary)
    Sub-classes must provide the transform() method to provide the conversion of a folder to 0 or
    more new binary files and metadata.
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        This simply stores the given instance in this instance for later use.
        """
        self.config = config

    def transform(
        self, folder_name: str
    ) -> tuple[list[tuple[bytes, str]], dict[str, Any]]:
        """
        Converts input folder into o or more output files.
        If there is an error, an exception must be raised - exit()ing is not generally allowed.
        :param folder_name: the name of the folder containing arbitrary amount of files.
        :return: a tuple of a list of 0 or more tuples and a dictionary of statistics that will be propagated
                to metadata.  Each element of the return list, is a tuple of the transformed bytes and a string
                holding the file name to use.
        """
        raise NotImplementedError()
