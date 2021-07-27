@ECHO OFF
python -m build
for /f "delims=" %%a in ('dir /b /od ".\dist\*"') do set file=%%a
python -m pip install ".\dist\%file%"
ECHO "%file%"
ECHO.
ECHO.
ECHO Done packaging and installing!
timeout /t 1 > NUL
