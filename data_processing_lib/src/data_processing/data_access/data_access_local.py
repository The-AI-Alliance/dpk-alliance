import gzip
import json
import os
from pathlib import Path
from typing import Any

from data_processing.data_access import DataAccess
from data_processing.utils import get_logger


logger = get_logger(__name__)


class DataAccessLocal(DataAccess):
    """
    Implementation of the Base Data access class for local folder data access.
    """

    def __init__(
        self,
        local_config: dict[str, str] = None,
        checkpoint: bool = False,
        m_files: int = -1,
        n_samples: int = -1,
        files_to_use: list[str] = [".parquet"],
        files_to_checkpoint: list[str] = [".parquet"],
    ):
        """
        Create data access class for folder based configuration
        :param local_config: dictionary of path info
        :param checkpoint: flag to return only files that do not exist in the output directory
        :param m_files: max amount of files to return
        :param n_samples: amount of files to randomly sample
        :param files_to_use: files extensions of files to include
        :param files_to_checkpoint: files extensions of files to use for checkpointing
        """
        super().__init__(
            checkpoint=checkpoint,
            m_files=m_files,
            n_samples=n_samples,
            files_to_use=files_to_use,
            files_to_checkpoint=files_to_checkpoint,
        )
        if local_config is None:
            self.input_folder = None
            self.output_folder = None
        else:
            if local_config["input_folder"] is None:
                self.input_folder = None
            else:
                self.input_folder = os.path.abspath(local_config["input_folder"])
            if local_config["output_folder"] is None:
                self.output_folder = None
            else:
                self.output_folder = os.path.abspath(local_config["output_folder"])
        logger.debug(f"Local input folder: {self.input_folder}")
        logger.debug(f"Local output folder: {self.output_folder}")
        logger.debug(f"Local checkpoint: {self.checkpoint}")
        logger.debug(f"Local m_files: {self.m_files}")
        logger.debug(f"Local n_samples: {self.n_samples}")
        logger.debug(f"Local files_to_use: {self.files_to_use}")
        logger.debug(f"Local files_to_checkpoint: {self.files_to_checkpoint}")

    def get_output_folder(self) -> str:
        """
        Get output folder as a string
        :return: output_folder
        """
        return self.output_folder

    def get_input_folder(self) -> str:
        """
        Get input folder as a string
        :return: input_folder
        """
        return self.input_folder

    def _list_files_folder(self, path: str) -> tuple[list[dict[str, Any]], int]:
        """
        Get files for a given folder and all sub folders
        :param path: path
        :return: List of files
        """
        files = sorted(Path(path).rglob("*"))
        res = []
        for file in files:
            if file.is_dir():
                continue
            res.append({"name": str(file), "size": file.stat().st_size})
        return res, 0

    def save_job_metadata(self, metadata: dict[str, Any]) -> tuple[dict[str, Any], int]:
        """
        Save metadata
        :param metadata: a dictionary, containing the following keys:
            "job details",
            "job_input_params",
            "execution_stats",
            "job_output_stats"
        two additional elements:
            "source"
            "target"
        are filled by implementation
        :return: a dictionary as
        defined https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html
        in the case of failure dict is None
        """
        if self.output_folder is None:
            logger.error("local configuration is not defined, can't save metadata")
            return None, 0
        metadata["source"] = {"name": self.input_folder, "type": "path"}
        metadata["target"] = {"name": self.output_folder, "type": "path"}
        return self.save_file(
            path=os.path.join(self.output_folder, "metadata.json"),
            data=json.dumps(metadata, indent=2).encode(),
        )

    def get_file(self, path: str) -> tuple[bytes, int]:
        """
        Gets the contents of a file as a byte array, decompressing gz files if needed.

        Args:
            path (str): The path to the file.

        Returns:
            bytes: The contents of the file as a byte array, or None if an error occurs.
        """

        try:
            if path.endswith(".gz"):
                with gzip.open(path, "rb") as f:
                    data = f.read()
            else:
                with open(path, "rb") as f:
                    data = f.read()
            return data, 0

        except (FileNotFoundError, gzip.BadGzipFile) as e:
            logger.error(f"Error reading file {path}: {e}")
            raise e

    def save_file(self, path: str, data: bytes) -> tuple[dict[str, Any], int]:
        """
        Saves bytes to a file and returns a dictionary with file information.

        Args:
            data (bytes): The bytes data to save.
            path (str): The full name of the file to save.

        Returns:
            dict or None: A dictionary with "name" and "size" keys if successful,
                        or None if saving fails.
        """

        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as f:
                f.write(data)
            file_info = {"name": path, "size": os.path.getsize(path)}
            return file_info, 0

        except Exception as e:
            logger.error(f"Error saving bytes to file {path}: {e}")
            return None, 0
