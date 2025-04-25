clean::
	@# Help: Clean up the distribution build and the venv
	rm -rf dist test/output
	rm -rf build
	rm -rf *.egg-info

lint::
	ruff check

test_data_access::
	export PYTHONPATH=src ;  \
	pytest test/data_access

test_invoke::
	export PYTHONPATH=src ;  \
	pytest test/invoker

test_launch::
	export PYTHONPATH=src ;  \
	pytest test/launch

test_transform_python::
	export PYTHONPATH=src ;  \
	pytest test/transform/python

test_transform_ray::
	export PYTHONPATH=src ;  \
	pytest test/transform/ray

build:
	pip install .
