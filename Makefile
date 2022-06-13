PYTHON ?= python3
COVFAIL=80
IN_VENV=. ./venv/bin/activate


venv/bin/activate:
	test -d venv || $(PYTHON) -m venv venv --prompt pykraken2
	${IN_VENV} && pip install pip --upgrade
	${IN_VENV} && pip install -r requirements.txt

.PHONY: develop
develop: venv/bin/activate venv/bin/kraken2
	${IN_VENV} && pip install -e .


venv/bin/kraken2: venv/bin/activate
	cd kraken2 && ../install_kraken2.sh ../venv/bin


.PHONY: test
test: venv/bin/activate venv/bin/kraken2
	${IN_VENV} && pip install pytest pytest-cov flake8 flake8-rst-docstrings flake8-docstrings flake8-import-order flake8-forbid-visual-indent
	${IN_VENV} && flake8 pykraken2 \
		--import-order-style google --application-import-names pykraken2 \
		--statistics
	${IN_VENV} && pytest pykraken2 --doctest-modules \
		--cov=pykraken2 --cov-report html --cov-report term \
		--cov-fail-under=${COVFAIL} --cov-report term-missing

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

