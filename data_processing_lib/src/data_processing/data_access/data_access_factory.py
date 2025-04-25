import argparse
import uuid
import ast
from typing import Any, Union

from data_processing.data_access import (
    DataAccess,
    DataAccessHF,
    DataAccessLocal,
    DataAccessS3,
)
from data_processing.utils import CLIArgumentProvider, ParamsUtils, str2bool, get_logger


class DataAccessFactory(CLIArgumentProvider):
    """
    This class is accepting Data Access parameters, validates them and instantiates an appropriate
    Data Access class based on these parameters.
    This class has to be serializable, so that we can pass it to the actors
    """

    def __init__(self, cli_arg_prefix: str = "data_"):
        """
        Create the factory to parse a set of args that will then define the type of DataAccess object
        to be created by the create_data_access() method.
        :param cli_arg_prefix:  if provided, this will be prepended to all the CLI arguments names.
               Make sure it ends with _
        This allows the creation of transform-specific (or other) DataAccess instances based on the
        transform-specific prefix (e.g. bl_ for blocklist transform).  The resulting keys returned
        in get_input_params() will include the prefix.  The underlying AST or other values of those
        keys is not effected by the prefix.
        """
        super().__init__()
        self.s3_cred = None
        self.checkpointing = False
        self.max_files = -1
        self.n_samples = -1
        self.files_to_use = []
        self.files_to_checkpoint = []
        self.cli_arg_prefix = cli_arg_prefix
        self.logger = get_logger(__name__ + str(uuid.uuid4()))

        self.s3_config = None
        self.local_config = None
        self.hf_config = None

    def add_input_params(self, parser: argparse.ArgumentParser) -> None:
        """
        Define data access specific parameters
        The set of parameters here is a superset of parameters required for all
        supported data access. The user only needs to specify the ones that he needs
        the rest will have the default values
        This might need to be extended if new data access implementation is added
        :param parser: parser
        :return: None
        """

        help_example_dict = {
            "access_key": ["access", "access key help text"],
            "secret_key": ["secret", "secret key help text"],
            "url": [
                "https://s3.us-east.cloud-object-storage.appdomain.cloud",
                "optional s3 url",
            ],
            "region": ["us-east-1", "optional s3 region"],
        }
        parser.add_argument(
            f"--{self.cli_arg_prefix}s3_cred",
            type=ast.literal_eval,
            default=None,
            help="AST string of options for s3 credentials. Only required for S3 data access.\n"
            + ParamsUtils.get_ast_help_text(help_example_dict),
        )
        # s3 config
        help_example_dict = {
            "input_folder": [
                "s3-path/your-input-bucket",
                "Path to input folder of files to be processed",
            ],
            "output_folder": [
                "s3-path/your-output-bucket",
                "Path to output folder of processed files",
            ],
        }
        parser.add_argument(
            f"--{self.cli_arg_prefix}s3_config",
            type=ast.literal_eval,
            default=None,
            help="AST string containing input/output paths.\n"
            + ParamsUtils.get_ast_help_text(help_example_dict),
        )
        # local config
        help_example_dict = {
            "input_folder": [
                "./input",
                "Path to input folder of files to be processed",
            ],
            "output_folder": [
                "/tmp/output",
                "Path to output folder of processed files",
            ],
        }
        parser.add_argument(
            f"--{self.cli_arg_prefix}local_config",
            type=ast.literal_eval,
            default=None,
            help="ast string containing input/output folders using local fs.\n"
            + ParamsUtils.get_ast_help_text(help_example_dict),
        )
        # hf config
        help_example_dict = {
            "hf_token": ["./input", "HF token required for write operation"],
            "input_folder": [
                "./input",
                "Path to input folder of files to be processed",
            ],
            "output_folder": [
                "/tmp/output",
                "Path to output folder of processed files",
            ],
        }
        parser.add_argument(
            f"--{self.cli_arg_prefix}hf_config",
            type=ast.literal_eval,
            default=None,
            help="ast string containing hf_token/input/output folders using hf fs.\n"
            + ParamsUtils.get_ast_help_text(help_example_dict),
        )
        parser.add_argument(
            f"--{self.cli_arg_prefix}max_files",
            type=int,
            default=-1,
            help="Max amount of files to process",
        )
        parser.add_argument(
            f"--{self.cli_arg_prefix}checkpointing",
            type=lambda x: bool(str2bool(x)),
            default=False,
            help="checkpointing flag",
        )
        # In the case of binary files, the resulting extension can be different from the source extension
        # The checkpointing extension is defined here. If multiple files (extensions) are produced from the
        # source files, only the leading one is required here
        parser.add_argument(
            f"--{self.cli_arg_prefix}files_to_checkpoint",
            type=ast.literal_eval,
            default=ast.literal_eval("['.parquet']"),
            help="list of file extensions used for checkpointing.",
        )
        parser.add_argument(
            f"--{self.cli_arg_prefix}files_to_use",
            type=ast.literal_eval,
            default=ast.literal_eval("['.parquet']"),
            help="list of file extensions to choose for input.",
        )
        parser.add_argument(
            f"--{self.cli_arg_prefix}num_samples",
            type=int,
            default=-1,
            help="number of random input files to process",
        )

    def apply_input_params(self, args: Union[dict, argparse.Namespace]) -> bool:
        """
        Validate data access specific parameters
        This might need to be extended if new data access implementation is added
        :param args: user defined arguments
        :return: None
        """
        if isinstance(args, argparse.Namespace):
            arg_dict = vars(args)
        elif isinstance(args, dict):
            arg_dict = args
        else:
            raise ValueError("args must be Namespace or dictionary")
        s3_cred = arg_dict.get(f"{self.cli_arg_prefix}s3_cred", None)
        s3_config = arg_dict.get(f"{self.cli_arg_prefix}s3_config", None)
        local_config = arg_dict.get(f"{self.cli_arg_prefix}local_config", None)
        hf_config = arg_dict.get(f"{self.cli_arg_prefix}hf_config", None)
        checkpointing = arg_dict.get(f"{self.cli_arg_prefix}checkpointing", False)
        max_files = arg_dict.get(f"{self.cli_arg_prefix}max_files", -1)
        n_samples = arg_dict.get(f"{self.cli_arg_prefix}num_samples", -1)
        files_to_use = arg_dict.get(f"{self.cli_arg_prefix}files_to_use", [".parquet"])
        files_to_checkpoint = arg_dict.get(
            f"{self.cli_arg_prefix}files_to_checkpoint", [".parquet"]
        )
        # check which configuration (S3 or Local) is specified
        s3_config_specified = 1 if s3_config is not None else 0
        local_config_specified = 1 if local_config is not None else 0
        hf_config_specified = 1 if hf_config is not None else 0

        # check that only one (S3 or Local) configuration is specified
        if s3_config_specified + local_config_specified + hf_config_specified > 1:
            self.logger.error(
                f"data factory {self.cli_arg_prefix} "
                f"{'S3, ' if s3_config_specified == 1 else ''}"
                f"{'Local ' if local_config_specified == 1 else ''}"
                f"{'hf ' if hf_config_specified == 1 else ''}"
                "configurations specified, but only one configuration expected"
            )
            return False

        # further validate the specified configuration (S3, hf or Local)
        if s3_config_specified == 1:
            # S3 config requires S3 credentials
            if not self._validate_s3_cred(s3_credentials=s3_cred):
                return False
            self.s3_cred = s3_cred
            config = {"input_folder": None, "output_folder": None}
            input_folder = s3_config.get("input_folder", None)
            if input_folder == "":
                input_folder = None
            if input_folder is not None:
                config["input_folder"] = input_folder
            output_folder = s3_config.get("output_folder", None)
            if output_folder == "":
                output_folder = None
            if output_folder is not None:
                config["output_folder"] = s3_config.get("output_folder")
            if not self._validate_s3_config(s3_config=config):
                return False
            self.s3_config = config
            self.logger.info(
                f"data factory {self.cli_arg_prefix} is using S3 data access: "
                f"input path - {self.s3_config['input_folder']}, "
                f"output path - {self.s3_config['output_folder']}"
            )
        elif hf_config_specified == 1:
            config = {
                "input_folder": None,
                "output_folder": None,
                "hf_token": hf_config.get("hf_token", ""),
            }
            input_folder = hf_config.get("input_folder", None)
            if input_folder == "":
                input_folder = None
            if input_folder is not None:
                config["input_folder"] = input_folder
            output_folder = hf_config.get("output_folder", None)
            if output_folder == "":
                output_folder = None
            if output_folder is not None:
                config["output_folder"] = output_folder
            if not self._validate_hf_config(hf_config=config):
                return False
            self.hf_config = config
            self.logger.info(
                f"data factory {self.cli_arg_prefix} is using HF data access: "
                f"input_folder - {self.hf_config['input_folder']} "
                f"output_folder - {self.hf_config['output_folder']}"
            )
        elif s3_cred is not None:
            if not self._validate_s3_cred(s3_credentials=s3_cred):
                return False
            self.s3_cred = s3_cred
            self.logger.info(
                f"data factory {self.cli_arg_prefix} is using s3 configuration without input/output path"
            )
        elif local_config_specified == 1:
            config = {"input_folder": None, "output_folder": None}
            input_folder = local_config.get("input_folder", None)
            if input_folder == "":
                input_folder = None
            if input_folder is not None:
                config["input_folder"] = input_folder
            output_folder = local_config.get("output_folder", None)
            if output_folder == "":
                output_folder = None
            if output_folder is not None:
                config["output_folder"] = output_folder
            if not self._validate_local_config(local_config=config):
                return False
            self.local_config = config
            self.logger.info(
                f"data factory {self.cli_arg_prefix} is using local data access: "
                f"input_folder - {self.local_config['input_folder']} "
                f"output_folder - {self.local_config['output_folder']}"
            )
        else:
            self.logger.info(
                f"data factory {self.cli_arg_prefix} "
                f"is using local configuration without input/output path"
            )

        # Check whether both max_files and number samples are defined
        self.logger.info(
            f"data factory {self.cli_arg_prefix} max_files {max_files}, n_sample {n_samples}"
        )
        if max_files > 0 and n_samples > 0:
            self.logger.error(
                f"data factory {self.cli_arg_prefix} "
                f"Both max files {max_files} and random samples {n_samples} are defined. Only one allowed at a time"
            )
            return False
        self.checkpointing = checkpointing
        self.max_files = max_files
        self.n_samples = n_samples
        self.files_to_use = files_to_use
        self.files_to_checkpoint = files_to_checkpoint
        self.logger.info(
            f"data factory {self.cli_arg_prefix} "
            f"Checkpointing {checkpointing}, max files {max_files}, "
            f"random samples {n_samples}, files to use {files_to_use}, files to checkpoint {files_to_checkpoint}"
        )
        return True

    def create_data_access(self) -> DataAccess:
        """
        Create data access based on the parameters
        :return: corresponding data access class
        """
        if self.hf_config is not None:
            # hf-config is specified, its hf
            return DataAccessHF(
                hf_config=self.hf_config,
                checkpoint=self.checkpointing,
                m_files=self.max_files,
                n_samples=self.n_samples,
                files_to_use=self.files_to_use,
                files_to_checkpoint=self.files_to_checkpoint,
            )
        if self.s3_config is not None or self.s3_cred is not None:
            # If S3 config or S3 credential are specified, its S3
            return DataAccessS3(
                s3_credentials=self.s3_cred,
                s3_config=self.s3_config,
                checkpoint=self.checkpointing,
                m_files=self.max_files,
                n_samples=self.n_samples,
                files_to_use=self.files_to_use,
                files_to_checkpoint=self.files_to_checkpoint,
            )
        # anything else is local data
        return DataAccessLocal(
            local_config=self.local_config,
            checkpoint=self.checkpointing,
            m_files=self.max_files,
            n_samples=self.n_samples,
            files_to_use=self.files_to_use,
            files_to_checkpoint=self.files_to_checkpoint,
        )

    def get_input_params(self) -> dict[str, Any]:
        """
        get input parameters for job_input_params for metadata
        :return: dictionary of params
        """
        return {
            "checkpointing": self.checkpointing,
            "max_files": self.max_files,
            "random_samples": self.n_samples,
            "files_to_use": self.files_to_use,
            "files_to_checkpoint": self.files_to_checkpoint,
        }

    def _validate_s3_cred(self, s3_credentials: dict[str, str]) -> bool:
        """
        Validate that
        :param s3_credentials: dictionary of S3 credentials
        :return:
        """
        if s3_credentials is None:
            self.logger.error(
                f"data access factory {self.cli_arg_prefix}: missing s3_credentials"
            )
            return False
        valid_config = True
        if s3_credentials.get("access_key") is None:
            self.logger.error(
                f"data access factory {self.cli_arg_prefix}: missing S3 access_key"
            )
            valid_config = False
        if s3_credentials.get("secret_key") is None:
            self.logger.error(
                f"data access factory {self.cli_arg_prefix}: missing S3 secret_key"
            )
            valid_config = False
        return valid_config

    def _validate_local_config(self, local_config: dict[str, str]) -> bool:
        """
        Validate that
        :param local_config: dictionary of local config
        :return: True if local config is valid, False otherwise
        """
        if (
            local_config.get("input_folder") is None
            and local_config.get("output_folder") is None
        ):
            self.logger.error(
                f"data access factory {self.cli_arg_prefix}: Could not find input and output folder in local config"
            )
            return False

        return True

    def _validate_s3_config(self, s3_config: dict[str, str]) -> bool:
        """
        Validate that
        :param s3_config: dictionary of s3 config
        :return: True if s3 config is valid, False otherwise
        """
        if (
            s3_config.get("input_folder") is None
            and s3_config.get("output_folder") is None
        ):
            self.logger.error(
                f"data access factory {self.cli_arg_prefix}: Could not find input and output folder in s3 config"
            )
            return False
        return True

    def _validate_hf_config(self, hf_config: dict[str, str]) -> bool:
        """
        Validate that
        :param hf_config: dictionary of hf config
        :return: True if hf config is valid, False otherwise
        """
        valid_config = True
        if hf_config.get("hf_token", "") == "":
            self.logger.warning(
                f"data access factory {self.cli_arg_prefix}: "
                f"HF token is not defined, write operation may fail"
            )
        input_folder = hf_config.get("input_folder", None)
        output_folder = hf_config.get("output_folder", None)
        if input_folder is None and output_folder is None:
            valid_config = False
            self.logger.error(
                f"data access factory {self.cli_arg_prefix}: Could not find input and output folder in HF config"
            )
        if input_folder is not None and not input_folder.startswith("datasets/"):
            valid_config = False
            self.logger.error(
                f"data access factory {self.cli_arg_prefix}: "
                f"Input folder in HF config has to start from datasets/"
            )
        if output_folder is not None and not output_folder.startswith("datasets/"):
            valid_config = False
            self.logger.error(
                f"data access factory {self.cli_arg_prefix}: "
                f"Output folder in HF config has to start from datasets/"
            )
        return valid_config


if __name__ == "__main__":
    factory = DataAccessFactory()
    arg_parser = argparse.ArgumentParser(
        description="Driver for data factory",
        # RawText is used to allow better formatting of ast-based arguments
        # See uses of ParamsUtils.dict_to_str()
        formatter_class=argparse.RawTextHelpFormatter,
    )
    factory.add_input_params(parser=arg_parser)
    factory_args = arg_parser.parse_args()
    factory.apply_input_params(args=factory_args)
