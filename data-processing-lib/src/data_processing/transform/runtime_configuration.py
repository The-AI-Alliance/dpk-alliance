from argparse import ArgumentParser, Namespace
from typing import Any

from data_processing.transform import (
    AbstractTransform,
    TransformConfiguration,
    TransformRuntime,
)
from data_processing.utils import CLIArgumentProvider


class TransformRuntimeConfiguration(CLIArgumentProvider):
    def __init__(
        self,
        transform_config: TransformConfiguration,
        runtime_class: type[TransformRuntime],
    ):
        """
        Initialization
        :param transform_config - base configuration class
        :param runtime_class - base runtime class
        """
        super().__init__()
        self.transform_config = transform_config
        self.runtime_class = runtime_class

    def add_input_params(self, parser: ArgumentParser) -> None:
        self.transform_config.add_input_params(parser)

    def apply_input_params(self, args: Namespace) -> bool:
        return self.transform_config.apply_input_params(args)

    def get_input_params(self) -> dict[str, Any]:
        return self.transform_config.get_input_params()

    def get_transform_class(self) -> type[AbstractTransform]:
        """
        Get the class extending AbstractTransform which implements a specific transformation.
        The class will generally be instantiated with a dictionary of configuration produced by
        the associated TransformRuntime get_transform_config() method.
        :return: class extending AbstractTransform
        """
        return self.transform_config.get_transform_class()

    def get_name(self):
        return self.transform_config.get_name()

    def get_transform_metadata(self) -> dict[str, Any]:
        """
        Get transform metadata. Before returning remove all parameters key accumulated in
        self.remove_from metadata. This allows transform developer to mark any input parameters
        that should not make it to the metadata. This can be parameters containing sensitive
        information, access keys, secrets, passwords, etc.
        :return parameters for metadata:
        """
        return self.transform_config.get_transform_metadata()

    def get_transform_params(self) -> dict[str, Any]:
        """
         Get transform parameters
        :return: transform parameters
        """
        return self.transform_config.get_transform_params()

    def create_transform_runtime(self) -> TransformRuntime:
        """
        Create transform runtime with the parameters captured during apply_input_params()
        :return: transform runtime object
        """
        return self.runtime_class(self.transform_config.get_transform_params())
