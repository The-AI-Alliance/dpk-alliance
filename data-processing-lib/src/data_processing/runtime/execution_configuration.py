import argparse

from data_processing.utils import CLIArgumentProvider, get_logger


logger = get_logger(__name__)


runtime_cli_prefix = "runtime_"


class TransformExecutionConfiguration(CLIArgumentProvider):
    """
    A class specifying and validating transform execution configuration
    """

    def __init__(self, name: str, print_params: bool = True):
        """
        Initialization
        :param name: job name
        :param print_params: flag to print parameters
        """
        super().__init__()
        self.job_details = {}
        self.name = name
        self.print_params = print_params

    def add_input_params(self, parser: argparse.ArgumentParser) -> None:
        """
        This method adds transformer specific parameters
        :param parser: parser
        :return:
        """
        parser.add_argument(
            f"--{runtime_cli_prefix}job_category",
            type=str,
            default="preprocessing",
            help="job category",
        )

    def apply_input_params(self, args: argparse.Namespace) -> bool:
        """
        Validate transformer specific parameters
        :param args: user defined arguments
        :return: True, if validate pass or False otherwise
        """
        captured = CLIArgumentProvider.capture_parameters(
            args, runtime_cli_prefix, False
        )
        # store parameters locally
        self.job_details = {
            "job category": captured["job_category"],
            "job name": self.name,
            "job type": "pure python",
        }
        # print parameters
        if self.print_params:
            logger.info(f"job details {self.job_details}")
        return True
