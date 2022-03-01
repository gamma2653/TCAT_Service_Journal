VENV = venv
VENV_BIN = $(VENV)/Scripts
# VENV_BIN = $(VENV)/bin
PYTHON = $(VENV_BIN)/python
PIP = $(VENV_BIN)/pip
# NOTE: Gunicorn is not supported on Windows
GUNICORN = $(VENV_BIN)/gunicorn
GUNICORN_PROD = $(GUNICORN) --workers 4 --bind 0.0.0.0:8000
# NOTE: Waitress is a fallback if Gunicorn is not available
WAITRESS = $(VENV_BIN)/waitress-serve
WAITRESS_PROD = $(WAITRESS) --listen=0.0.0.0:8000
WAITRESS_DEV = $(WAITRESS) --listen=*:8080
# NOTE: Flask development server is not intended for development
FLASK_APP = service_journal.server_tools.app:create_app
FLASK = $(VENV_BIN)/flask run
FLASK_ENV = development
WEB_SERVER = $(FLASK)

export FLASK_APP
export FLASK_ENV

.PHONY: run clean setup

$(VENV_BIN)/activate: requirements.txt
	python -m venv $(VENV)
	$(PIP) install -r requirements.txt

setup: requirements.txt
	$(PIP) install -r requirements.txt

clean:
	rd /s /q $(VENV)
	rd /s /q __pycache__
# rm -rf $(VENV)
# rm -rf __pycache__

run: $(VENV_BIN)/activate
	$(WEB_SERVER) 