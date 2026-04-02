@echo off
title APL Document Pipeline — Export Excel
cd /d "C:\Development\Document-Pipeline"

echo.
echo ============================================================
echo  APL Document Pipeline
echo  Regenerating Index, Masterlist and Tracker
echo ============================================================
echo.

python pipeline\export_index.py

echo.
echo ============================================================
echo  Done. Files written to OneDrive exports folder.
echo ============================================================
echo.
pause
