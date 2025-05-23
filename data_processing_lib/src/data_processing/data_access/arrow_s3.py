from typing import Any

import boto3
from botocore.config import Config
from data_processing.utils import get_logger


logger = get_logger(__name__)


class ArrowS3:
    """
    Class replacing direct access to S3/COS by Pyarrow's `fs.S3FileSystem`. It uses Boto3 to interact
    with S3/COS and pyarrow to convert between Arrow table and binary. Usage of Boto3 for S3/COS access
    proves to be significantly more reliable
    """

    def __init__(
        self,
        access_key: str,
        secret_key: str,
        endpoint: str = None,
        region: str = None,
        s3_retries: int = 10,
        s3_max_attempts=10,
    ) -> None:
        """
        Initialization
        :param access_key: s3 access key
        :param secret_key: s3 secret key
        :param endpoint: s3 endpoint
        :param region: s3 region
        :param s3_retries: number of S3 retries - default 10
        :param s3_max_attempts - boto s3 client internal retries - default 10
        """
        # Create boto S3 client
        self.s3_client = boto3.client(
            service_name="s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            endpoint_url=endpoint,
            region_name=region,
            config=Config(
                retries={"max_attempts": s3_max_attempts, "mode": "standard"}
            ),
        )
        self.retries = s3_retries
        self.s3_max_attempts = s3_max_attempts

    @staticmethod
    def _get_bucket_key(key: str) -> tuple[str, str]:
        """
        Splitting complete folder name into bucket and prefix used by boto3 client
        :param key: complete folder name
        :return: bucket name and prefix
        """
        prefixes = key.split("/")
        return prefixes[0], "/".join(prefixes[1:])

    # get list of the files (names and sizes) for a given prefix (including bucket name)
    def list_files(self, key: str) -> tuple[list[dict[str, Any]], int]:
        """
        List files in the folder (hierarchically going through all sub-folders)
        :param key: complete folder name
        :return: list of dictionaries, containing file names and length and number of retries
        """
        bucket, prefix = self._get_bucket_key(key)
        # Use paginator here to get all the files rather than 1 page
        paginator = self.s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        files = []
        retries = 0
        for page in pages:
            # For every page
            retries += page.get("ResponseMetadata", {}).get("RetryAttempts", 0)
            for obj in page.get("Contents", []):
                # Get both file name and size
                files.append({"name": f"{bucket}/{obj['Key']}", "size": obj["Size"]})
        return files, retries

    def list_folders(self, key: str) -> tuple[list[str], int]:
        """
        Get list of folders for folder
        :param key: complete folder
        :return: list of folders within a given folder and number of retries
        """

        def _get_sub_folders(bck: str, p: str) -> tuple[list[str], int]:
            sub_folders = []
            # use paginator
            paginator = self.s3_client.get_paginator("list_objects_v2")
            # use Delimiter to get folders just folders
            page_iterator = paginator.paginate(Bucket=bck, Prefix=p, Delimiter="/")
            internal_retries = 0
            for page in page_iterator:
                # for every page
                internal_retries += page.get("ResponseMetadata", {}).get(
                    "RetryAttempts", 0
                )
                for p in page.get("CommonPrefixes", []):
                    sf = p["Prefix"]
                    sub_folders.append(sf)
                    # apply recursively
                    sf, r = _get_sub_folders(bck=bck, p=sf)
                    internal_retries += r
                    sub_folders.extend(sf)
            return sub_folders, internal_retries

        bucket, prefix = self._get_bucket_key(key)
        subs, retries = _get_sub_folders(bck=bucket, p=prefix)
        return [f"{bucket}/{f}" for f in subs], retries

    def read_file(self, key: str) -> tuple[bytes, int]:
        """
        Read s3 file by name
        :param key: complete path
        :return: byte array of file content or None if the file does not exist and a number of retries
        """
        bucket, prefix = self._get_bucket_key(key)
        retries = 0
        for n in range(self.retries):
            try:
                obj = self.s3_client.get_object(Bucket=bucket, Key=prefix)
                retries += obj.get("ResponseMetadata", {}).get("RetryAttempts", 0)
                return obj["Body"].read(), retries
            except Exception as e:
                logger.error(f"failed to read file {key}, exception {e}, attempt {n}")
                retries += self.s3_max_attempts
        logger.error(
            f"failed to read file {key} in {self.retries} attempts. Skipping it"
        )
        return None, retries

    def save_file(self, key: str, data: bytes) -> tuple[dict[str, Any], int]:
        """
        Save file to S3
        :param key: complete path
        :param data: byte array of the file content
        :return: dictionary as
        defined https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html
        in the case of failure dict is None and the number of retries
        """
        bucket, prefix = self._get_bucket_key(key)
        retries = 0
        for n in range(self.retries):
            try:
                res = self.s3_client.put_object(Bucket=bucket, Key=prefix, Body=data)
                retries += res.get("ResponseMetadata", {}).get("RetryAttempts", 0)
                return res, retries
            except Exception as e:
                logger.error(f"Failed to upload file to to key {key}, exception {e}")
                retries += self.s3_max_attempts
        logger.error(f"Failed to upload file {key}, skipping it")
        return None, retries

    def delete_file(self, key: str) -> int:
        """
        Delete file from S3
        :param key: complete path
        :return: the number of retries
        """
        bucket, prefix = self._get_bucket_key(key)
        retries = 0
        for n in range(self.retries):
            try:
                res = self.s3_client.delete_object(Bucket=bucket, Key=prefix)
                retries += res.get("ResponseMetadata", {}).get("RetryAttempts", 0)
                return retries
            except Exception as e:
                logger.error(f"failed to delete file {key}, exception {e}")
                retries += self.s3_max_attempts
        return retries

    def move_file(self, source: str, dest: str) -> int:
        """
        move file from source to destination
        :param source: complete source path
        :param dest: complete destination path
        :return: number of retries
        """
        s_bucket, s_prefix = self._get_bucket_key(source)
        d_bucket, d_prefix = self._get_bucket_key(dest)
        # copy source to destination and then delete source
        copy_source = {"Bucket": s_bucket, "Key": s_prefix}
        retries = 0
        for n in range(self.retries):
            try:
                res = self.s3_client.copy_object(
                    CopySource=copy_source, Bucket=d_bucket, Key=d_prefix
                )
                retries += res.get("ResponseMetadata", {}).get("RetryAttempts", 0)
                retries += self.delete_file(source)
                return retries
            except Exception as e:
                logger.error(f"failed to copy file {source} to {dest}, exception {e}")
                retries += self.s3_max_attempts
        return retries
