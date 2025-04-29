clean::
	@# Help: Clean up the distribution build and the venv
	rm -rf dist test/output
	rm -rf build
	rm -rf src/*.egg-info
	rm -f .coverage

lint::
	ruff check

format::
	ruff format

test_data_access::
	export PYTHONPATH=src ;  \
	pytest test/data_access --cov

test_invoke::
	export PYTHONPATH=src ;  \
	pytest test/invoker --cov

test_launch::
	export PYTHONPATH=src ;  \
	pytest test/launch --cov

test_transform_python::
	export PYTHONPATH=src ;  \
	pytest test/transform/python --cov

test_transform_ray::
	export PYTHONPATH=src ;  \
	pytest test/transform/ray --cov

build::
	pip install .

build_dist::
	rm -rf dist || true
	rm -rf src/*egg-info || true
	pip install --upgrade build
	python3.10 -m build

publish::
	python3.10 -m twine check dist/*
	python3.10 -m twine upload --verbose --non-interactive dist/*