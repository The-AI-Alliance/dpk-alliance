import sys
from typing import Any

from data_processing.runtime import AbstractTransformLauncher
from data_processing.transform import TransformRuntimeConfiguration
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
        launcher: AbstractTransformLauncher,
    ):
        """
        Initialization
        :param launcher: transform launcher
        """
        self.launcher = launcher

    def invoke_transform(
        self,
        name: str,
        transform_config: TransformRuntimeConfiguration,
        params: dict[str, Any],
    ) -> bool:
        """
        Invoke transform
        :param name: transform name
        :param transform_config: transform configuration class
        :param params: transform parameters
        :return:
        """
        # Set the command line args
        current_args = sys.argv
        sys.argv = ParamsUtils.dict_to_req(d=params)
        # create transform specific launcher
        try:
            launcher = self.launcher(runtime_config=transform_config)
            # Launch the ray actor(s) to process the input
            res = launcher.launch()
        except (Exception, SystemExit) as e:
            logger.warning(f"Exception executing transform {name}: {e}")
            res = 1
        # restore args
        sys.argv = current_args
        return res
