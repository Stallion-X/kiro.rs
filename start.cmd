@echo off
REM kiro-rs launcher. Double-click = foreground run. Args pass through to start.ps1 (e.g. -Background / -Stop)
chcp 65001 >nul
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0start.ps1" %*
