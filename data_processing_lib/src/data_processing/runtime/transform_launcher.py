import argparse
import sys
from typing import Any, Union

from data_processing.data_access import DataAccessFactory
from data_processing.runtime import TransformOrchestrator
from data_processing.transform import TransformRuntimeConfiguration
from data_processing.utils import ParamsUtils, get_logger


logger = get_logger(__name__)


class AbstractTransformLauncher:
    """
    Abstract transform Launcher
    """

    def __init__(
        self,
        runtime_config: TransformRuntimeConfiguration,
        # If two data access factories are specified, we assume input first, output second
        data_access_factory: Union[
            DataAccessFactory, list[DataAccessFactory]
        ] = DataAccessFactory(),
        orchestrator: type[TransformOrchestrator] = TransformOrchestrator,
    ):
        """
        Creates driver
        :param runtime_config: transform runtime factory
        :param data_access_factory: the factory(s) to create DataAccess instances.
        :param orchestrator: implementation of the orchestrator
        """
        self.runtime_config = runtime_config
        self.name = self.runtime_config.get_name()
        if isinstance(data_access_factory, DataAccessFactory):
            # If a single data access factory is specified, used it for both input and output
            self.data_access_factory = [data_access_factory, data_access_factory]
        else:
            self.data_access_factory = data_access_factory
        self.orchestrator = orchestrator
        self.execution_config = None

    def _get_parser(self) -> argparse.ArgumentParser:
        """
        parser creation
        :return: parser
        """
        return argparse.ArgumentParser(
            description=f"Driver for {self.name} processing",
            # RawText is used to allow better formatting of ast-based arguments
            # See uses of ParamsUtils.dict_to_str()
            formatter_class=argparse.RawTextHelpFormatter,
        )

    def _get_arguments(self, parser: argparse.ArgumentParser) -> argparse.Namespace:
        """
        Parse input parameters
        :param parser: parser
        :return: list of arguments
        """
        # add additional arguments
        self.runtime_config.add_input_params(parser=parser)
        if self.data_access_factory[0] == self.data_access_factory[1]:
            self.data_access_factory[0].add_input_params(parser=parser)
        else:
            for daf in self.data_access_factory:
                daf.add_input_params(parser=parser)
        self.execution_config.add_input_params(parser=parser)
        return parser.parse_args()

    def _get_parameters(self, args: argparse.Namespace) -> bool:
        """
        This method creates arg parser, fills it with the parameters
        and does parameters validation
        :return: True if validation passes or False, if not
        """
        result = True
        for daf in self.data_access_factory:
            result = result and daf.apply_input_params(args=args)
        return (
            result
            and self.runtime_config.apply_input_params(args=args)
            and self.execution_config.apply_input_params(args=args)
        )

    def _submit_for_execution(self) -> int:
        """
        Submit for execution
        :return:
        """
        raise ValueError("must be implemented by subclass")

    def launch(self):
        """
        Execute method orchestrates driver invocation
        :return:
        """
        args = self._get_arguments(self._get_parser())
        if self._get_parameters(args):
            return self._submit_for_execution()
        return 1

    def get_transform_name(self) -> str:
        return self.name


def multi_launcher(params: dict[str, Any], launcher: AbstractTransformLauncher) -> int:
    """
    Multi launcher. A function orchestrating multiple launcher executions
    :param params: A set of parameters containing an array of configs (s3, local, etc.)
    :param launcher: An actual launcher for a specific runtime
    :return: number of launches
    """
    # find config parameter
    config = ParamsUtils.get_config_parameter(params)
    if config is None:
        return 1
    # get and validate config value
    config_value = params[config]
    if type(config_value) is not list:
        logger.warning("config value is not a list")
        return 1
    # remove config key from the dictionary
    launch_params = dict(params)
    del launch_params[config]
    # Loop through all parameters
    n_launches = 0
    for conf in config_value:
        # populate individual config and launch
        launch_params[config] = conf
        sys.argv = ParamsUtils.dict_to_req(d=launch_params)
        res = launcher.launch()
        if res > 0:
            logger.warning(f"Launch with configuration {conf} failed")
        else:
            n_launches += 1
    return n_launches
