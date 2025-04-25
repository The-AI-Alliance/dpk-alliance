import argparse
import sys
import time
from typing import Union

import ray
from data_processing.data_access import DataAccessFactory
from data_processing.runtime import AbstractTransformLauncher, orchestrate
from data_processing.utils import get_logger, str2bool
from data_processing.runtime.ray import (
    RayTransformExecutionConfiguration,
    RayTransformOrchestrator,
    RayTransformRuntimeConfiguration,
)


logger = get_logger(__name__)


class RayTransformLauncher(AbstractTransformLauncher):
    """
    Driver class starting Filter execution
    """

    def __init__(
        self,
        runtime_config: RayTransformRuntimeConfiguration,
        data_access_factory: Union[
            DataAccessFactory, list[DataAccessFactory]
        ] = DataAccessFactory(),
        orchestrator: type[RayTransformOrchestrator] = RayTransformOrchestrator,
        address: str = "ray://localhost:10001",
    ):
        """
        Creates driver
        :param runtime_config: transform runtime factory
        :param data_access_factory: the factory to create DataAccess instances.
        :param orchestrator: orchestrator class to use
        :param address: GRPC access to the existing Ray cluster
        """
        super().__init__(
            runtime_config=runtime_config,
            data_access_factory=data_access_factory,
            orchestrator=orchestrator,
        )
        self.execution_config = RayTransformExecutionConfiguration(name=self.name)
        self.address = address

    def _get_arguments(self, parser: argparse.ArgumentParser) -> argparse.Namespace:
        """
        Parse input parameters
        :param parser: parser
        :return: list of arguments
        """
        parser.add_argument(
            "--run_locally",
            type=lambda x: bool(str2bool(x)),
            default=False,
            help="running ray local flag",
        )
        return super()._get_arguments(parser)

    def _get_parameters(self, args: argparse.Namespace) -> bool:
        """
        This method creates arg parser, fill it with the parameters
        and does parameters validation
        :return: True id validation passes or False, if not
        """
        result = super()._get_parameters(args)
        self.run_locally = args.run_locally
        if self.run_locally:
            logger.info("Running locally")
        else:
            logger.info("connecting to existing cluster")
        return result

    def _submit_for_execution(self) -> int:
        """
        Submit for Ray execution
        :return:
        """
        res = 1
        start = time.time()
        try:
            if self.run_locally:
                # Will create a local Ray cluster
                logger.debug("running locally creating Ray cluster")
                # enable metrics for local Ray
                ray.init(_metrics_export_port=8088)
            else:
                # connect to the existing cluster
                logger.info("Connecting to the existing Ray cluster")
                ray.init(address=self.address, ignore_reinit_error=True)
            logger.debug("Starting orchestrator")
            remote_orchestrate = ray.remote(num_cpus=1, scheduling_strategy="SPREAD")(
                orchestrate
            )
            res = ray.get(
                remote_orchestrate.remote(
                    execution_params=self.execution_config,
                    data_access_factory=self.data_access_factory,
                    runtime_config=self.runtime_config,
                    orchestrator=self.orchestrator,
                )
            )
            logger.debug("Completed orchestrator")
            time.sleep(10)
        except Exception as e:
            logger.info(f"Exception running ray remote orchestration\n{e}")
        finally:
            logger.info(
                f"Completed execution in {round((time.time() - start) / 60.0, 3)} min, execution result {res}"
            )
            ray.shutdown()
            return res

    def launch(self) -> int:
        """
        Execute method orchestrates driver invocation
        :return: launch result
        """
        res = super().launch()
        if not self.run_locally and res > 0:
            # if we are running in kfp exit to signal kfp that we failed
            sys.exit(1)
        return res
