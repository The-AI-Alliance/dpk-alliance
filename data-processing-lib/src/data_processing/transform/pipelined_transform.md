# Pipelined Transform

Typical DPK usage is a sequential invocation of individual transforms that process all the input data and create
an output data set. Such execution is very convenient as it produces all the intermediate data, which can be useful,
especially when debugging is required.

That said, such an approach creates a lot of intermediate data and executes a lot of reads and writes, which might
significantly slow down processing, especially in the case of large data sets.

To overcome this drawback, DPK introduced an additional type of transform - pipeline transform. Pipeline transform
(which is somewhat similar to [sklearn pipeline](https://scikit-learn.org/1.5/modules/generated/sklearn.pipeline.Pipeline.html))
is both a transform, meaning it transforms one file at a time, and a pipeline, meaning that each file is processed by
a set of individual transformers, passing data between then as a byte array in memory.

>***Note*** When defining pipeline transform each base transform can be used in pipeline only once. If you need to use 
a given transform more then once you need to create a copy of this transform configuration with a different 
configuration prefix and different configuration parameters (see example 
[here](../examples/noop/python/noop1_transform.py))

## Pipeline Transform configuration

As pipelined transform is a type of transform it is using a configuration class to configure its execution.
Because pipeline transform is a special type of transform, it is using a special 
[configuration implementation](pipeline_transform_configuration.py). The only parameters specified 
for a pipelined transforms is a list of transform configurations (specific for transform runtime).
Here list can have both individual transform configuration or arrays of transform configurations. 
In the latter case transforms in sublist are considered to be a fork and are executed on the same data.
The diagram below shows the execution:

```text
The configuration [a, b, c] is converted to

a -> b -> c

The configuration [a, [b, c], d] is converted to

a -> b -> d
     c ->
```

After the fork completion, data produced by fork participants is merged, for example fork of two
transforms each producing a single file will return 2 files.
Configurations of the participating transforms are defined by the configurations of the transforms 
themselves.

This implementation might seem limited - it only supports forks with a single transform. In reality this is
not the case as you can create a pipeline transform using another pipeline transforms as participants, which
allows you to implement any acyclic graph (no direct support for conditional execution, even this can be implemented 
by using special transform, with conditional return) of any complexity

> ***Note:*** As per DPK convention, parameters for every transform are prefixed by a transform name, which means 
that a given transform will always get an appropriate parameter.
