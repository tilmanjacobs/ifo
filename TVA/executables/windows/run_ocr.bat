@echo off
setlocal

:: Set python path to use .venv from parent directory
set PYTHON_PATH=%~dp0..\.venv\Scripts\python.exe

:ask
set /p test_mode="Do you want to run in test mode? (y/n): "
if /i "%test_mode%"=="y" goto run_test
if /i "%test_mode%"=="n" goto run_normal
echo Please enter 'y' for yes or 'n' for no
goto ask

:run_test
echo.
echo Running ocr_2.py in test mode...
"%PYTHON_PATH%" ocr_2.py --test
if errorlevel 1 goto error
echo Successfully completed ocr_2.py
goto run_pdf

:run_normal
echo.
echo Running ocr_2.py...
"%PYTHON_PATH%" ocr_2.py
if errorlevel 1 goto error
echo Successfully completed ocr_2.py

:run_pdf
echo.
echo Running pdf_to_txt.py...
"%PYTHON_PATH%" pdf_to_txt.py
if errorlevel 1 goto error
echo Successfully completed pdf_to_txt.py
goto end

:error
echo Error occurred while running the scripts
pause
exit /b 1

:end
echo.
echo All scripts completed successfully
pause 