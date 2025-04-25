import argparse
from typing import Any

from data_processing.runtime import TransformExecutionConfiguration
from data_processing.utils import CLIArgumentProvider, get_logger


logger = get_logger(__name__)


cli_prefix = "runtime_"


class PythonTransformExecutionConfiguration(TransformExecutionConfiguration):
    """
    A class specifying and validating Python orchestrator configuration
    """

    def __init__(self, name: str):
        """
        Initialization
        """
        super().__init__(name=name, print_params=False)
        self.num_processors = 0

    def add_input_params(self, parser: argparse.ArgumentParser) -> None:
        """
        This method adds transformer specific parameters
        :param parser: parser
        :return:
        """
        parser.add_argument(
            f"--{cli_prefix}num_processors",
            type=int,
            default=0,
            help="size of multiprocessing pool",
        )

        return TransformExecutionConfiguration.add_input_params(self, parser=parser)

    def apply_input_params(self, args: argparse.Namespace) -> bool:
        """
        Validate transformer specific parameters
        :param args: user defined arguments
        :return: True, if validate pass or False otherwise
        """
        if not TransformExecutionConfiguration.apply_input_params(self, args=args):
            return False
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, False)
        # store parameters locally
        self.num_processors = captured["num_processors"]
        # print them
        if self.num_processors > 0:
            # we are using multiprocessing
            logger.info(f"using multiprocessing, num processors {self.num_processors}")
        return True

    def get_input_params(self) -> dict[str, Any]:
        """
        get input parameters for job_input_params in metadata
        :return: dictionary of parameters
        """
        return {"num_processors": self.num_processors}
