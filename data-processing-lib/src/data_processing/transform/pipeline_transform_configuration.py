from argparse import ArgumentParser, Namespace
from typing import Any

from data_processing.transform import (
    AbstractPipelineTransform,
    TransformConfiguration,
    TransformRuntimeConfiguration,
)
from data_processing.utils import get_logger


logger = get_logger(__name__)


class PipelineTransformConfiguration(TransformConfiguration):
    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args.
    """

    def __init__(
        self,
        pipeline: list[TransformRuntimeConfiguration],
        transform_class: type[AbstractPipelineTransform],
        name: str = "pipeline",
    ):
        super().__init__(
            name=name,
            transform_class=transform_class,
        )
        self.pipeline = pipeline

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        This will be included in a dictionary used to initialize the Transform.
        By convention a common prefix should be used for all transform-specific CLI args
        (e.g, noop_, pii_, etc.)
        """
        for t in self.pipeline:
            t.transform_config.add_input_params(parser=parser)

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        res = True
        for t in self.pipeline:
            res = res and t.transform_config.apply_input_params(args=args)
        return res

    def get_input_params(self) -> dict[str, Any]:
        """
        Provides a default implementation if the user has provided a set of keys to the initializer.
        These keys are used in apply_input_params() to extract our key/values from the global Namespace of args.
        :return:
        """
        params = {}
        for t in self.pipeline:
            params |= t.transform_config.get_input_params()
        return params

    def get_transform_metadata(self) -> dict[str, Any]:
        """
        Get transform metadata. Before returning remove all parameters key accumulated in
        self.remove_from metadata. This allows transform developer to mark any input parameters
        that should not make it to the metadata. This can be parameters containing sensitive
        information, access keys, secrets, passwords, etc.
        :return parameters for metadata:
        """
        params = {}
        for t in self.pipeline:
            params |= t.transform_config.get_transform_metadata()
        return params
