
## Install Python Dependencies
requirements: test_environment
	pip install -r requirements.txt


## Create virtualenv using virtualenvwrapper
venv:
	mkvirtualenv -r requirements.txt -a . $(PROJECT_NAME)


## Test python environment is setup correctly
test_environment:
	python test_environment.py


