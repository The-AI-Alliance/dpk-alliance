import time
from typing import Union

from data_processing.data_access import DataAccessFactory
from data_processing.runtime import orchestrate
from data_processing.runtime.python import (
    PythonTransformExecutionConfiguration,
    PythonTransformOrchestrator,
    PythonTransformRuntimeConfiguration,
)
from data_processing.runtime.transform_launcher import AbstractTransformLauncher
from data_processing.utils import get_logger


logger = get_logger(__name__)


class PythonTransformLauncher(AbstractTransformLauncher):
    """
    Driver class starting Filter execution
    """

    def __init__(
        self,
        runtime_config: PythonTransformRuntimeConfiguration,
        data_access_factory: Union[
            DataAccessFactory, list[DataAccessFactory]
        ] = DataAccessFactory(),
        orchestrator: type[PythonTransformOrchestrator] = PythonTransformOrchestrator,
    ):
        """
        Creates driver
        :param runtime_config: transform runtime factory
        :param data_access_factory: the factory to create DataAccess instances.
        """
        super().__init__(
            runtime_config=runtime_config,
            data_access_factory=data_access_factory,
            orchestrator=orchestrator,
        )
        self.execution_config = PythonTransformExecutionConfiguration(
            name=runtime_config.get_name()
        )

    def _submit_for_execution(self) -> int:
        """
        Submit for execution
        :return:
        """
        res = 1
        start = time.time()
        try:
            logger.debug("Starting orchestrator")
            res = orchestrate(
                data_access_factory=self.data_access_factory,
                runtime_config=self.runtime_config,
                execution_params=self.execution_config,
                orchestrator=self.orchestrator,
            )
            logger.debug("Completed orchestrator")
        except Exception as e:
            logger.info(f"Exception running orchestration\n{e}")
        finally:
            logger.info(
                f"Completed execution in {round((time.time() - start) / 60.0, 3)} min, execution result {res}"
            )
            return res
