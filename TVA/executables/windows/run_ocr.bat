@echo off
setlocal

:: Set python path to use .venv from parent directory of TVA
set PYTHON_PATH=%~dp0..\..\..\.venv\Scripts\python.exe

:: Change to TVA directory where the Python scripts are
cd %~dp0..\..

:ask
set /p test_mode="Do you want to run in test mode? (y/n): "
if /i "%test_mode%"=="y" goto run_test
if /i "%test_mode%"=="n" goto run_normal
echo Please enter 'y' for yes or 'n' for no
goto ask

:run_test
echo.
echo Running ocr.py in test mode...
"%PYTHON_PATH%" python/ocr.py --test
if errorlevel 1 goto error
echo Successfully completed ocr.py
goto run_pdf

:run_normal
echo.
echo Running ocr.py...
"%PYTHON_PATH%" python/ocr.py
if errorlevel 1 goto error
echo Successfully completed ocr.py

:run_pdf
echo.
echo Running pdf_to_txt.py...
"%PYTHON_PATH%" python/pdf_to_txt.py
if errorlevel 1 goto error
echo Successfully completed pdf_to_txt.py

:run_csv
echo.
echo Running txt_to_csv.py...
"%PYTHON_PATH%" python/txt_to_csv.py
if errorlevel 1 goto error
echo Successfully completed txt_to_csv.py
goto end

:error
echo Error occurred while running the scripts
pause
exit /b 1

:end
echo.
echo All scripts completed successfully
pause 