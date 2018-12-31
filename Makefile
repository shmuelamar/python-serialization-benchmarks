SHELL := $(shell which bash)
PROJECT := serialization-bench
ACTIVATE_VENV = source activate ${PROJECT}
ENV_FILE := "conda-env.yml"

rm-env:
	-conda env remove -y --name ${PROJECT}

setup-env:
	-conda create -y --name ${PROJECT}
	conda env update --name ${PROJECT} --file ${ENV_FILE}

bench:
	${ACTIVATE_VENV} && \
		export PYTHONPATH=./$(PROJECT):$$PYTHONPATH && \
		python benchmarks.py

validate:
	${ACTIVATE_VENV} && flake8 $(PROJECT)/ tests/

all:
	$(error please pick a target)

.PHONY: clean validate
