import sys
from typing import Any, Union

from data_processing.runtime import AbstractTransformLauncher
from data_processing.transform import TransformRuntimeConfiguration
from data_processing.data_access import DataAccessFactory
from data_processing.utils import (
    ParamsUtils,
    get_logger,
)


logger = get_logger(__name__)


class TransformInvoker:
    """
    Abstract transform invoker. Used to invoke transform as an API
    """

    def __init__(
        self,
        launcher: type[AbstractTransformLauncher],
    ):
        """
        Initialization
        :param launcher: transform launcher class
        """
        self.launcher = launcher

    def invoke_transform(
        self,
        name: str,
        runtime_config: TransformRuntimeConfiguration,
        params: dict[str, Any],
        data_access_factory: Union[
            DataAccessFactory, list[DataAccessFactory]
        ] = DataAccessFactory(),
    ) -> bool:
        """
        Invoke transform
        :param name: transform name
        :param runtime_config: transform configuration
        :param params: transform parameters
        :param data_access_factory: the factory(s) to create DataAccess instances
        :return:
        """
        # Set the command line args
        current_args = sys.argv
        sys.argv = ParamsUtils.dict_to_req(d=params)
        # create transform specific launcher
        try:
            launcher = self.launcher(
                runtime_config=runtime_config, data_access_factory=data_access_factory
            )
            # Launch the ray actor(s) to process the input
            res = launcher.launch()
        except (Exception, SystemExit) as e:
            logger.warning(f"Exception executing transform {name}: {e}")
            res = 1
        # restore args
        sys.argv = current_args
        return res
