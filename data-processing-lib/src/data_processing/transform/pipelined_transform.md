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

## Pipeline Transform configuration

As pipelined transform is a type of transform it is using a configuration class to configure its execution.
Because pipeline transform is a special type of transform, it is using a special 
[configuration implementation](pipeline_transform_configuration.py). The only parameters specified 
for a pipelined transforms is a list of transform configurations (specific for transform runtime). 
Configurations of the participating transforms are defined by the configurations of the transforms 
themselves.

> ***Note:*** As per DPK convention, parameters for every transform are prefixed by a transform name, which means 
that a given transform will always get an appropriate parameter. Also, if you want a given transform to participate
in several pipelines, but with different configurations, it has to have different prefixes.
