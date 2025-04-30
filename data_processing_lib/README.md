# Data Processing Library
This provides a python framework for developing _transforms_
on data stored in files and running them in a pure Python or [ray](https://www.ray.io/) cluster.
Data files may be stored in the local file system or  COS/S3.
For more details see the [documentation](../README.md).

## Library Artifact Build and Publish
To build the library
```shell
make build_dist
```
To publish it to PyPi
```shell
make publish
```

To up the version number, edit the Makefile to change VERSION and rerun
the above.  This will require committing both the `Makefile` and the
autotmatically updated `pyproject.toml` file.



