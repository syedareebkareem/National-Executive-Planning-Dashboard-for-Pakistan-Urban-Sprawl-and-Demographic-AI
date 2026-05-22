@echo off
SETLOCAL ENABLEDELAYEDEXPANSION

:: ============================================================
::   PAKISTAN URBAN SPRAWL PROJECT - MASTER PIPELINE RUNNER
:: ============================================================

:: Force the working directory to the PROJECT ROOT
cd /d "%~dp0"
SET PROJECT_ROOT=%cd%

:: Kill any stuck PySpark Java processes from previous crashed runs
taskkill /F /IM java.exe >nul 2>&1

:: SMART FOLDER DETECTION
echo %PROJECT_ROOT% | findstr /i "\\src" >nul
if %errorlevel%==0 (
    SET SCRIPTS_DIR=%PROJECT_ROOT%
    SET LOG_FILE=%PROJECT_ROOT%\..\pipeline_log.txt
) else (
    SET SCRIPTS_DIR=%PROJECT_ROOT%\src
    SET LOG_FILE=%PROJECT_ROOT%\pipeline_log.txt
)

SET PASS=0
SET FAIL=0

echo ============================================================ > "%LOG_FILE%"
echo   PAKISTAN URBAN SPRAWL - PIPELINE LOG                       >> "%LOG_FILE%"
echo   Started: %date% %time%                                     >> "%LOG_FILE%"
echo ============================================================ >> "%LOG_FILE%"

echo.
echo  ====================================================
echo    PAKISTAN URBAN SPRAWL ^& DEMOGRAPHIC AI PIPELINE
echo  ====================================================
echo.
echo  Scripts Folder: %SCRIPTS_DIR%
echo.

IF NOT EXIST "%SCRIPTS_DIR%" (
    echo  [ERROR] src folder not found at: %SCRIPTS_DIR%
    pause
    EXIT /B 1
)

echo  [OK] Scripts folder identified.
echo  [INFO] Running pipeline...
echo.
pause

:: --- RUNNER LOGIC ---
CALL :RUN_SCRIPT  1  "ingest.py"          "Data Ingestion"
CALL :RUN_SCRIPT  2  "preprocess.py"      "Spatial Data Cleaning"
CALL :RUN_SCRIPT  3  "features.py"        "Feature Engineering"
CALL :RUN_SCRIPT  4  "model.py"           "Random Forest Training"
CALL :RUN_SCRIPT  5  "predict.py"         "2030 Sprawl Prediction"
CALL :RUN_SCRIPT  6  "demographics.py"    "Demographic Projections"
CALL :RUN_SCRIPT  7  "risk_dashboard.py"  "Risk Ranking Report"
CALL :RUN_SCRIPT  8  "kmeans_centers.py"  "K-Means City Centers"
CALL :RUN_SCRIPT  9  "peri_urban.py"      "Peri-Urban Classification"
CALL :RUN_SCRIPT 10  "evaluate_models.py" "Model Evaluation Report"
CALL :RUN_SCRIPT 11  "charts.py"          "Matplotlib/Seaborn Charts"
CALL :RUN_SCRIPT 12  "visualize.py"       "Folium Map Generation"

:SUMMARY
echo.
echo  ====================================================
echo    PIPELINE SUMMARY: %PASS% Passed / %FAIL% Failed
echo  ====================================================
if %FAIL% EQU 0 (
    echo  [SUCCESS] All systems operational.
) else (
    echo  [ERROR] Check pipeline_log.txt for Python traceback.
)
pause
ENDLOCAL
EXIT /B

:RUN_SCRIPT
    SET SCRIPT_PATH=%SCRIPTS_DIR%\%~2
    echo  STEP %~1: %~3...
    IF NOT EXIST "%SCRIPT_PATH%" (
        echo   [FAIL] Missing: %~2
        SET /A FAIL+=1
        EXIT /B 1
    )
    
    :: Pause for 2 seconds to let OS release file locks from the previous PySpark script
    timeout /t 2 /nobreak >nul
    
    python "%SCRIPT_PATH%" >> "%LOG_FILE%" 2>&1
    IF !ERRORLEVEL! EQU 0 (
        echo   [PASS] Done.
        SET /A PASS+=1
    ) ELSE (
        echo   [FAIL] Python Error!
        SET /A FAIL+=1
        GOTO SUMMARY
    )
    EXIT /B 0