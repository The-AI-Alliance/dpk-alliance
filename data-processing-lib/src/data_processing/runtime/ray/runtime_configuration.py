from data_processing.transform import (
    TransformConfiguration,
    TransformRuntimeConfiguration,
)
from data_processing.transform.ray import DefaultRayTransformRuntime


class RayTransformRuntimeConfiguration(TransformRuntimeConfiguration):
    def __init__(
        self,
        transform_config: TransformConfiguration,
        runtime_class: type[DefaultRayTransformRuntime] = DefaultRayTransformRuntime,
    ):
        """
        Initialization
        :param transform_config - base configuration class
        :param runtime_class: implementation of the transform runtime
        """
        super().__init__(transform_config=transform_config, runtime_class=runtime_class)
