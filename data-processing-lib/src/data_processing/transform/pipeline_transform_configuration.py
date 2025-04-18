from argparse import ArgumentParser, Namespace
from typing import Any, Union

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
        pipeline: list[
            Union[TransformRuntimeConfiguration, list[TransformRuntimeConfiguration]]
        ],
        transform_class: type[AbstractPipelineTransform],
        name: str = "pipeline",
    ):
        """
        Initialise pipeline configuration
        :param pipeline: definition of the pipeline, supporting both sequential and fork/join
                         implementation. Here, an internal list specifies transforms running in parallel
        :param transform_class: base implementation class
        :param name: transform name
        """
        super().__init__(
            name=name,
            transform_class=transform_class,
        )
        # convert definition to array of arrays
        p_arrays = []
        for p in pipeline:
            if isinstance(p, TransformRuntimeConfiguration):
                # single transform
                p_arrays.append([p])
            else:
                # its a list.
                p_arrays.append(p)
        # save array of arrays
        self.pipeline = p_arrays

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        This will be included in a dictionary used to initialize the Transform.
        By convention a common prefix should be used for all transform-specific CLI args
        (e.g, noop_, pii_, etc.)
        """
        for t in self.pipeline:
            for tt in t:
                tt.transform_config.add_input_params(parser=parser)

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        res = True
        for t in self.pipeline:
            for tt in t:
                res = res and tt.transform_config.apply_input_params(args=args)
        return res

    def get_input_params(self) -> dict[str, Any]:
        """
        Provides a default implementation if the user has provided a set of keys to the initializer.
        These keys are used in apply_input_params() to extract our key/values from the global Namespace of args.
        :return:
        """
        params = {}
        for t in self.pipeline:
            for tt in t:
                params |= tt.transform_config.get_input_params()
        return params

    def get_transform_params(self) -> dict[str, Any]:
        """
        Get transform parameters
        :return: transform parameters
        """
        return {"transforms": self.pipeline}

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
            for tt in t:
                params |= tt.transform_config.get_transform_metadata()
        return params
