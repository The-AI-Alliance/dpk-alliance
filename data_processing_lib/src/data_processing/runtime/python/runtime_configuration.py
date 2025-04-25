from data_processing.transform import (
    TransformConfiguration,
    TransformRuntimeConfiguration,
)
from data_processing.transform.python import DefaultPythonTransformRuntime


class PythonTransformRuntimeConfiguration(TransformRuntimeConfiguration):
    def __init__(
        self,
        transform_config: TransformConfiguration,
        runtime_class: type[
            DefaultPythonTransformRuntime
        ] = DefaultPythonTransformRuntime,
    ):
        """
        Initialization
        :param transform_config - base configuration class
        :param runtime_class: implementation of the transform runtime
        """
        self.runtime_class = runtime_class
        super().__init__(
            transform_config=transform_config,
            runtime_class=runtime_class,
        )

    def create_transform_runtime(self) -> DefaultPythonTransformRuntime:
        """
        Create transform runtime with the parameters captured during apply_input_params()
        :return: transform runtime object
        """
        return self.runtime_class(self.transform_config.get_transform_params())
