from typing import Any, Union

from data_processing.transform import (
    AbstractBinaryTransform,
    TransformRuntime,
    TransformRuntimeConfiguration,
)
from data_processing.utils import TransformUtils, UnrecoverableException, get_logger


class AbstractPipelineTransform(AbstractBinaryTransform):
    """
    Transform that executes a set of base transforms sequentially. Data is passed between
    participating transforms in memory
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initializes pipeline execution for the list of transforms
        :param config - configuration parameters - list of transforms in the pipeline.
        Note that transforms will be executed in the order they are defined
        """
        super().__init__({})
        self.logger = get_logger(__name__)
        transforms = config.get("transforms", [])
        if len(transforms) == 0:
            # Empty pipeline
            self.logger.error("Pipeline transform with empty list")
            raise UnrecoverableException("Pipeline transform with empty list")
        self.data_access_factory = config.get("data_access_factory", None)
        if self.data_access_factory is None:
            self.logger.error("pipeline transform - Data access factory is not defined")
            raise UnrecoverableException(
                "pipeline transform - Data access factory is not defined"
            )
        self.statistics = config.get("statistics", None)
        if self.statistics is None:
            self.logger.error("pipeline transform - Statistics is not defined")
            raise UnrecoverableException(
                "pipeline transform - Statistics is not defined"
            )
        self.transforms = transforms
        participants = []
        # for every transform in the pipeline
        for transform in transforms:
            f_join = []
            for t in transform:
                tr, runtime = self._create_transform(definition=t)
                f_join.append((tr, runtime))
            participants.append(f_join)
        # save participating transforms
        self.participants = participants
        self.file_name = ""

    def _create_transform(
        self, definition: TransformRuntimeConfiguration
    ) -> tuple[AbstractBinaryTransform, TransformRuntime]:
        """
        Create a transform and its runtime based on the definition
        :param definition: transform definition
        :return: transform and its runtime
        """
        # create runtime
        runtime = definition.create_transform_runtime()
        # get parameters
        transform_params = self._get_transform_params(runtime)
        # Create transform
        tr = definition.get_transform_class()(transform_params)
        return tr, runtime

    def _get_transform_params(self, runtime: TransformRuntime) -> dict[str, Any]:
        """
        get transform parameters
        :param runtime - runtime
        :return: transform params
        """
        raise NotImplementedError("must be implemented by subclass")

    def transform_binary(
        self, file_name: str, byte_array: bytes
    ) -> tuple[list[tuple[bytes, str]], dict[str, Any]]:
        """
        Converts input file into one or more output files.
        If there is an error, an exception must be raised - exiting is not generally allowed.
        :param byte_array: contents of the input file to be transformed.
        :param file_name: the name of the file containing the given byte_array.
        :return: a tuple of a list of 0 or more tuples and a dictionary of statistics that will be propagated
                to metadata.  Each element of the return list, is a tuple of the transformed bytes and a string
                holding the extension to be used when writing out the new bytes.
        """
        # process transforms sequentially
        self.file_name = file_name
        data = [(byte_array, file_name)]
        stats = {}
        for transform in self.participants:
            data, stats = self._execute_transform(
                transform=transform, data=data, stats=stats
            )
            if len(data) == 0:
                # no data returned by this transform
                return [], stats
        # all done
        return self._convert_output(data), stats

    def _execute_transform(
        self,
        transform: Union[
            tuple[AbstractBinaryTransform, TransformRuntime],
            list[tuple[AbstractBinaryTransform, TransformRuntime]],
        ],
        data: list[tuple[bytes, str]],
        stats: dict[str, Any],
    ) -> tuple[list[tuple[bytes, str]], dict[str, Any]]:
        """
        Execute a single or fork/join of transforms
        :param transform: single or list of transform for forking
        :param data: source data
        :param stats: source stats
        :return: resulting data and statistics
        """
        res = []
        for t in transform:
            dt, st = self._process_transform(transform=t[0], data=data)
            # Accumulate stats
            stats |= st
            res.append(dt)
        data = self.merge_fork_results(data=res)
        return data, stats

    @staticmethod
    def merge_fork_results(
        data: list[list[tuple[bytes, str]]],
    ) -> list[tuple[bytes, str]]:
        """
        Merging fork results. We assume only a single fork in the overall pipeline.
        This is a very simple implementation that just flattens array of arrays. For
        all other use cases this method has to be overwritten
        :param data: list of data returned by fork execution
        :return: merged data
        """
        res_data = []
        for dt in data:
            res_data += dt
        return res_data

    @staticmethod
    def _convert_output(data: list[tuple[bytes, str]]) -> list[tuple[bytes, str]]:
        res = [None] * len(data)
        i = 0
        for dt in data:
            f_name = TransformUtils.get_file_extension(dt[1])
            res[i] = (dt[0], f_name[1])
            i += 1
        return res

    @staticmethod
    def _process_transform(
        transform: AbstractBinaryTransform, data: list[tuple[bytes, str]]
    ) -> tuple[list[tuple[bytes, str]], dict[str, Any]]:
        """
        Process individual transform. Note here that the predecessor could create multiple data objects
        :param transform - transform
        :param data - data to process
        :return: resulting data and statistics
        """
        stats = {}
        res = []
        for dt in data:
            # for every data element
            src = TransformUtils.get_file_extension(dt[1])
            out_files, st = transform.transform_binary(
                byte_array=dt[0], file_name=dt[1]
            )
            # Accumulate results
            for ouf in out_files:
                res.append((ouf[0], src[0] + ouf[1]))
            # accumulate statistics
            stats |= st
        return res, stats

    def flush_binary(self) -> tuple[list[tuple[bytes, str]], dict[str, Any]]:
        """
        This is supporting method for transformers, that implement buffering of data, for example coalesce.
        These transformers can have buffers containing data that were not written to the output immediately.
        Flush is the hook for them to return back locally stored data and their statistics.
        The majority of transformers are expected not to use such buffering and can use this default implementation.
        If there is an error, an exception must be raised - calling exit() is not generally allowed.
        :return: a tuple of a list of 0 or more tuples and a dictionary of statistics that will be propagated
                to metadata.  Each element of the return list, is a tuple of the transformed bytes and a string
                holding the extension to be used when writing out the new bytes.
        """
        stats = {}
        res = []
        i = 0
        for transform in self.participants:
            partial = []
            for t in transform:
                dt, st = t[0].flush_binary()
                # Accumulate stats
                stats |= st
                partial.append(dt)
            out_files = self.merge_fork_results(data=partial)
            if len(out_files) > 0 and i < len(self.participants) - 1:
                # flush produced output - run it through the rest of the chain
                data = []
                for ouf in out_files:
                    data.append((ouf[0], self.file_name))
                for n in range(i + 1, len(self.participants)):
                    data, stats = self._execute_transform(
                        transform=self.participants[n], data=data, stats=stats
                    )
                    if len(data) == 0:
                        # no data returned by this transform
                        break
                    res += self._convert_output(data)
            else:
                res += out_files
            i += 1
        # Done flushing, compute execution stats
        self._compute_execution_statistics(stats)
        return res, {}

    def _compute_execution_statistics(self, stats: dict[str, Any]) -> None:
        """
        Compute execution statistics
        :param stats: current statistics from flush
        :return: None
        """
        raise NotImplementedError("must be implemented by subclass")
