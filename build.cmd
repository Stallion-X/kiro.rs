@echo off
REM kiro-rs one-click build + configure (double-click to run)
chcp 65001 >nul
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0build.ps1" %*
echo.
pause
