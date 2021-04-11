setup:
	python3 -m venv ~/.spark-airbnb
install:
	pip3 install --upgrade pip &&\
	pip3 install -r requirements.txt --user

format:
	python3 -m black spark-airbnb/*.py tests/*.py

test:
	python3 -m pytest -vv --cov=spark-airbnb tests/*.py

lint:
	python3 -m pylint --disable=R,C tests/*.py spark-airbnb/*.py
all: install lint test
