from argparse import ArgumentParser, Namespace

from data_processing.runtime.python import PythonTransformLauncher
from data_processing.runtime.python.runtime_configuration import (
    PythonTransformRuntimeConfiguration,
)
from data_processing.transform import (
    AbstractTransform,
    TransformConfiguration,
)
from data_processing.utils import CLIArgumentProvider, get_logger
from data_processing.examples.noop.python import NOOPTransform, sleep_key, pwd_key


logger = get_logger(__name__)

short_name = "noop1"
cli_prefix = f"{short_name}_"
sleep_cli_param = f"{cli_prefix}{sleep_key}"
pwd_cli_param = f"{cli_prefix}{pwd_key}"


class NOOP1TransformConfiguration(TransformConfiguration):
    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args.
    """

    def __init__(self, clazz: type[AbstractTransform] = NOOPTransform):
        super().__init__(
            name=short_name,
            transform_class=clazz,
            remove_from_metadata=[pwd_key],
        )

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        This will be included in a dictionary used to initialize the NOOPTransform.
        By convention a common prefix should be used for all transform-specific CLI args
        (e.g, noop_, pii_, etc.)
        """
        parser.add_argument(
            f"--{sleep_cli_param}",
            type=int,
            default=1,
            help="Sleep actor for a number of seconds while processing the data frame, before writing the file to COS",
        )
        # An example of a command line option that we don't want included
        # in the metadata collected by the Ray orchestrator
        # See below for remove_from_metadata addition so that it is not reported.
        parser.add_argument(
            f"--{pwd_cli_param}",
            type=str,
            default="nothing",
            help="A dummy password which should be filtered out of the metadata",
        )

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, False)
        if captured.get(sleep_key) < 0:
            print(
                f"Parameter noop1_sleep_sec should be non-negative. you specified {args.noop_sleep_sec}"
            )
            return False

        self.params = self.params | captured
        logger.info(f"noop1 parameters are : {self.params}")
        return True


class NOOP1PythonTransformConfiguration(PythonTransformRuntimeConfiguration):
    """
    Implements the PythonTransformConfiguration for NOOP as required by the PythonTransformLauncher.
    NOOP does not use a RayRuntime class so the superclass only needs the base
    python-only configuration.
    """

    def __init__(self):
        """
        Initialization
        """
        super().__init__(transform_config=NOOP1TransformConfiguration())


if __name__ == "__main__":
    launcher = PythonTransformLauncher(NOOP1PythonTransformConfiguration())
    logger.info("Launching noop1 transform")
    launcher.launch()
