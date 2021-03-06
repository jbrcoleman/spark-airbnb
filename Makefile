setup:
	python3 -m venv ~/.spark-airbnb
install:
	pip3 install --upgrade pip &&\
	pip3 install -r requirements.txt --user

format:
	python3 -m black spark_airbnb/*.py tests/*.py

test:
	python3 -m pytest -vv --cov=spark_airbnb tests/*.py

lint:
	python3 -m pylint --disable=R,C tests/*.py spark_airbnb/*.py
all: install lint test
