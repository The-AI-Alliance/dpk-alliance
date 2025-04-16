# DPK Alliance

This project implements a simplification of and extensions to the [Data Prep Kit](https://github.com/data-prep-kit/data-prep-kit) project to better support 
[AI Alliance](https://github.com/The-AI-Alliance/) needs, especially for the [Open Trusted Data Initiative](https://the-ai-alliance.github.io/open-trusted-data-initiative/).

Data Prep Kit is a community project to democratize and accelerate unstructured data preparation for LLM app developers. 
With the explosive growth of LLM-enabled use cases, developers are faced with the enormous challenge of preparing use 
case-specific unstructured data to fine-tune, instruct-tune the LLMs or to build RAG applications for LLMs. As the 
variety of use cases grow, so does the need to support:

* New ways of transforming the data to enhance the performance of the resulting LLMs for each specific use case.
* A large variety in the scale of data to be processed, from laptop-scale to datacenter-scale
* Support for different data modalities including language, code, vision, multimodal etc

To get more information about implementation architecture, please read this 
[blog post](https://thealliance.ai/blog/architecture-of-data-prep-kit-framework).

The main components of the framework are:
* [transforms](data-processing-lib/src/data_processing/transform/transform.md) - base classes for transforms creation
* [runtime](data-processing-lib/src/data_processing/runtime/runtime.md) - implementation of the runtime responsible for starting and executing transforms
* [data access](data-processing-lib/src/data_processing/data_access/data-access.md) - extendable implementation of configurable data access, including local files, S3 and Hugging Face data sets 

We also provide several examples of the framework usage:
* [NOOP](data-processing-lib/src/data_processing/examples/noop/README.md)
* [Resize](data-processing-lib/src/data_processing/examples/resize/README.md)
* [Pipeline](data-processing-lib/src/data_processing/examples/pipeline/README.md)


The project's code was extensively [tested](data-processing-lib/test) leveraging 
[transform testing framework](data-processing-lib/src/data_processing/test_support/README.md)