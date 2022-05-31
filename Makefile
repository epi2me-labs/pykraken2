.PHONY: develop docs

PYTHON ?= python3

IN_VENV=. ./venv/bin/activate

venv/bin/activate:
	test -d venv || $(PYTHON) -m venv venv
	${IN_VENV} && pip install pip --upgrade
	${IN_VENV} && pip install -r requirements.txt

develop: venv/bin/activate
	${IN_VENV} && python setup.py develop

test: venv/bin/activate
	${IN_VENV} && pip install flake8 flake8-rst-docstrings flake8-docstrings flake8-import-order flake8-forbid-visual-indent
	${IN_VENV} && flake8 kraken_server \
		--import-order-style google --application-import-names kraken_server \
		--statistics
	# we should install without error
	${IN_VENV} && python setup.py install

IN_BUILD=. ./pypi_build/bin/activate
pypi_build/bin/activate:
	test -d pypi_build || $(PYTHON) -m venv pypi_build --prompt "(pypi) "
	${IN_BUILD} && pip install pip --upgrade
	${IN_BUILD} && pip install --upgrade pip setuptools twine wheel readme_renderer[md] keyrings.alt

.PHONY: sdist
sdist: pypi_build/bin/activate
	${IN_BUILD} && python setup.py sdist

.PHONY: clean
clean:
	rm -rf __pycache__ dist build venv kraken_server.egg-info tmp docs/_build

