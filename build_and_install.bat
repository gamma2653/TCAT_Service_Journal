@ECHO OFF
python3 setup.py sdist
pip3 install ./dist/service_journal-2.0a0.tar.gz
ECHO.
ECHO.
ECHO Done packaging and installing!
timeout /t 1 > NUL