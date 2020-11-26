@ECHO OFF
python setup.py sdist
python -m pip install ./dist/service_journal-2.0a0.tar.gz
ECHO.
ECHO.
ECHO Done packaging and installing!
timeout /t 1 > NUL
